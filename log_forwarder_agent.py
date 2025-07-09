# agents/log_forwarder_agent.py
import snowflake.connector
import boto3
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from .base_agent import BaseAgent
from config import SnowflakeConfig, AWSConfig, LOG_CONFIG

class LogForwarderAgent(BaseAgent):
    """Agent responsible for forwarding logs from Snowflake to CloudWatch"""
    
    def __init__(self):
        super().__init__("LogForwarder", "LOG_FORWARDER")
        self.snowflake_config = None
        self.aws_config = None
        self.connection = None
        self.cloudwatch = None
        self.sequence_tokens = {}
        self.last_timestamps = {}
        
    def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize the log forwarder with configurations"""
        try:
            self.snowflake_config = SnowflakeConfig()
            self.aws_config = AWSConfig()
            
            # Initialize CloudWatch client
            self.cloudwatch = boto3.client('logs', region_name=self.aws_config.region)
            
            # Initialize last timestamps for each view
            self.last_timestamps = {
                view: datetime.now(timezone.utc) - timedelta(minutes=5)
                for view in LOG_CONFIG
            }
            
            # Connect to Snowflake
            self.connection = self._connect_to_snowflake()
            
            self.update_status("initialized", "Log forwarder initialized successfully")
            self.logger.info("Log forwarder agent initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize log forwarder: {e}")
            self.update_status("error", f"Initialization failed: {e}")
            return False
    
    def _connect_to_snowflake(self):
        """Establish connection to Snowflake"""
        self.logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=self.snowflake_config.user,
            password=self.snowflake_config.password,
            account=self.snowflake_config.account,
            role=self.snowflake_config.role,
            warehouse=self.snowflake_config.warehouse,
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema
        )
        self.logger.info("Connected to Snowflake")
        return conn
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute log forwarding task"""
        try:
            self.update_status("running", "Forwarding logs from Snowflake to CloudWatch")
            
            results = {}
            for log_type, time_col in LOG_CONFIG.items():
                result = self._fetch_and_forward_logs(log_type, time_col)
                results[log_type] = result
            
            self.update_status("completed", "Log forwarding cycle completed")
            return {
                "status": "success",
                "results": results,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error in log forwarding: {e}")
            self.update_status("error", f"Log forwarding failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _fetch_and_forward_logs(self, view_name: str, timestamp_col: str) -> Dict[str, Any]:
        """Fetch logs from Snowflake and forward to CloudWatch"""
        log_group = f"{self.aws_config.log_group_prefix}{view_name}"
        log_stream = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        
        self._ensure_log_group_exists(log_group)
        self._create_log_stream(log_group, log_stream)
        
        cursor = self.connection.cursor()
        try:
            query = f"""
            SELECT *
            FROM {self.snowflake_config.database}.{self.snowflake_config.schema}.{view_name}
            WHERE {timestamp_col} > %s
            AND {timestamp_col} IS NOT NULL
            ORDER BY {timestamp_col} ASC
            LIMIT 100;
            """
            
            cursor.execute(query, (self.last_timestamps[view_name],))
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            
            if not rows:
                self.logger.info(f"No new logs for {view_name}")
                return {"logs_processed": 0, "status": "no_new_logs"}
            else:
                self.logger.info(f"Found {len(rows)} new logs for {view_name}")
                for row in rows:
                    message = f"{view_name} LOG ENTRY\n"
                    for col, val in zip(columns, row):
                        message += f"{col}: {val}\n"
                    self._send_log_event(log_group, log_stream, message)
                
                self.last_timestamps[view_name] = rows[-1][columns.index(timestamp_col)]
                return {"logs_processed": len(rows), "status": "success"}
                
        except Exception as e:
            self.logger.error(f"Error fetching {view_name}: {e}")
            return {"logs_processed": 0, "status": "error", "error": str(e)}
        finally:
            cursor.close()
    
    def _ensure_log_group_exists(self, log_group: str):
        """Ensure CloudWatch log group exists"""
        try:
            groups = self.cloudwatch.describe_log_groups(logGroupNamePrefix=log_group)
            if not any(group['logGroupName'] == log_group for group in groups.get('logGroups', [])):
                self.cloudwatch.create_log_group(logGroupName=log_group)
                self.logger.info(f"Created log group: {log_group}")
        except Exception as e:
            self.logger.error(f"Error ensuring log group exists: {e}")
    
    def _create_log_stream(self, log_group: str, stream_name: str):
        """Create log stream if it doesn't exist"""
        try:
            self.cloudwatch.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
            self.logger.debug(f"Created log stream: {log_group}/{stream_name}")
        except self.cloudwatch.exceptions.ResourceAlreadyExistsException:
            self.logger.debug(f"Log stream already exists: {log_group}/{stream_name}")
        except Exception as e:
            self.logger.error(f"Error creating log stream: {e}")
    
    def _send_log_event(self, log_group: str, log_stream: str, message: str):
        """Send log event to CloudWatch"""
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        kwargs = {
            'logGroupName': log_group,
            'logStreamName': log_stream,
            'logEvents': [{
                'timestamp': timestamp_ms,
                'message': message
            }]
        }
        
        try:
            if log_stream in self.sequence_tokens:
                kwargs['sequenceToken'] = self.sequence_tokens[log_stream]
            
            response = self.cloudwatch.put_log_events(**kwargs)
            self.sequence_tokens[log_stream] = response['nextSequenceToken']
            self.logger.debug(f"Sent log to {log_group}/{log_stream}")
            
        except self.cloudwatch.exceptions.InvalidSequenceTokenException as e:
            expected = str(e).split("expected sequenceToken is: ")[-1]
            self.sequence_tokens[log_stream] = expected
            kwargs['sequenceToken'] = expected
            response = self.cloudwatch.put_log_events(**kwargs)
            self.sequence_tokens[log_stream] = response['nextSequenceToken']
            self.logger.info(f"Retried log to {log_group}/{log_stream} after token fix")
        except Exception as e:
            self.logger.error(f"Failed to send log to {log_group}/{log_stream}: {e}")