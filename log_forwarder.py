import snowflake.connector
import boto3
from datetime import datetime, timedelta, timezone
import logging
from autogen import AssistantAgent
import os
from dotenv import load_dotenv

load_dotenv()

class LogForwarderAgent(AssistantAgent):
    def __init__(self, name, llm_config):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("FORWARDER")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.USER = 'keerthana'
        self.PASSWORD = 'Quadrantkeerthana2025'
        self.ACCOUNT = 'pcc86913.us-east-1'
        self.ROLE = 'ACCOUNTADMIN'
        self.WAREHOUSE = 'COMPUTE_WH'
        self.DATABASE = 'SNOWFLAKE'
        self.SCHEMA = 'ACCOUNT_USAGE'
        self.cloudwatch = boto3.client('logs', region_name='us-east-1')
        self.LOG_CONFIG = {
            "QUERY_HISTORY": "START_TIME",
            "LOGIN_HISTORY": "EVENT_TIMESTAMP",
            "TASK_HISTORY": "SCHEDULED_TIME",
            "GRANTS_TO_USERS": "CREATED_ON",
            "WAREHOUSE_LOAD_HISTORY": "START_TIME"
        }
        self.last_timestamps = {
            view: datetime.now(timezone.utc) - timedelta(minutes=5)
            for view in self.LOG_CONFIG
        }
        self.sequence_token = {}
        self.conn = None
        try:
            self.conn = self.connect_to_snowflake()
        except Exception as e:
            self.logger.error(f"Failed to initialize Snowflake connection: {e}")
            with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] Initialization error: {e}\n{'-' * 60}\n")

    def ensure_log_group_exists(self, log_group):
        self.logger.debug(f"Checking if log group {log_group} exists")
        groups = self.cloudwatch.describe_log_groups(logGroupNamePrefix=log_group)
        if not any(group['logGroupName'] == log_group for group in groups.get('logGroups', [])):
            self.cloudwatch.create_log_group(logGroupName=log_group)
            self.logger.info(f"Created log group: {log_group}")

    def create_log_stream(self, log_group, stream_name):
        try:
            self.cloudwatch.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
            self.logger.debug(f"Created log stream: {log_group}/{stream_name}")
        except self.cloudwatch.exceptions.ResourceAlreadyExistsException:
            self.logger.debug(f"Log stream already exists: {log_group}/{stream_name}")

    def send_log_event(self, log_group, log_stream, message):
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
            if log_stream in self.sequence_token:
                kwargs['sequenceToken'] = self.sequence_token[log_stream]
            response = self.cloudwatch.put_log_events(**kwargs)
            self.sequence_token[log_stream] = response['nextSequenceToken']
            self.logger.info(f"Sent log to {log_group}/{log_stream}")
        except self.cloudwatch.exceptions.InvalidSequenceTokenException as e:
            expected = str(e).split("expected sequenceToken is: ")[-1]
            self.sequence_token[log_stream] = expected
            kwargs['sequenceToken'] = expected
            response = self.cloudwatch.put_log_events(**kwargs)
            self.sequence_token[log_stream] = response['nextSequenceToken']
            self.logger.info(f"Retried log to {log_group}/{log_stream} after token fix")
        except Exception as e:
            self.logger.error(f"Failed to send log to {log_group}/{log_stream}: {e}")

    def fetch_and_forward_logs(self, view_name, timestamp_col):
        if not self.conn:
            self.logger.warning(f"Skipping {view_name} fetch: No Snowflake connection")
            return
        log_group = f"/snowflake/{view_name}"
        log_stream = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        self.ensure_log_group_exists(log_group)
        self.create_log_stream(log_group, log_stream)
        cursor = self.conn.cursor()
        try:
            query = f"""
            SELECT *
            FROM {self.DATABASE}.{self.SCHEMA}.{view_name}
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
            else:
                self.logger.info(f"Found {len(rows)} new logs for {view_name}")
                for row in rows:
                    message = f"{view_name} LOG ENTRY\n"
                    for col, val in zip(columns, row):
                        message += f"{col}: {val}\n"
                    self.send_log_event(log_group, log_stream, message)
                self.last_timestamps[view_name] = rows[-1][columns.index(timestamp_col)]
        except Exception as e:
            self.logger.error(f"Error fetching {view_name}: {e}")
            with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] Error fetching {view_name}:\n{e}\n{'-' * 60}\n")
        finally:
            cursor.close()

    def connect_to_snowflake(self):
        self.logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT,
            role=self.ROLE,
            warehouse=self.WAREHOUSE,
            database=self.DATABASE,
            schema=self.SCHEMA
        )
        self.logger.info("Connected to Snowflake")
        return conn

    def run(self):
        if not self.conn:
            self.logger.warning("Snowflake connection unavailable, skipping log forwarding")
            return
        self.logger.info("Real-time log forwarding started...")
        try:
            while True:
                for log_type, time_col in self.LOG_CONFIG.items():
                    self.fetch_and_forward_logs(log_type, time_col)
                import time
                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.info("Log forwarding stopped by user")
        finally:
            if self.conn:
                self.conn.close()
                self.logger.info("Snowflake connection closed")