import boto3
import time
from datetime import datetime, timezone
from typing import Dict, Any, Set
from .base_agent import BaseAgent
from config import AWSConfig

class MonitorAgent(BaseAgent):
    """Agent responsible for monitoring CloudWatch logs for errors"""
    
    def __init__(self):
        super().__init__("Monitor", "MONITOR")
        self.aws_config = None
        self.cloudwatch = None
        self.seen_event_ids: Set[str] = set()
        
    def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize the monitor with configurations"""
        try:
            self.aws_config = AWSConfig()
            self.cloudwatch = boto3.client("logs", region_name=self.aws_config.region)
            
            self.update_status("initialized", "Monitor agent initialized successfully")
            self.logger.info("Monitor agent initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize monitor: {e}")
            self.update_status("error", f"Initialization failed: {e}")
            return False
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute monitoring task"""
        try:
            self.update_status("running", "Monitoring CloudWatch logs for errors")
            
            log_groups = self._get_recent_log_groups()
            errors_found = []
            
            for log_group in log_groups:
                group_errors = self._search_errors(log_group)
                if group_errors:
                    errors_found.extend(group_errors)
            
            result = {
                "status": "success",
                "log_groups_checked": len(log_groups),
                "errors_found": len(errors_found),
                "errors": errors_found,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if errors_found:
                self.update_status("alert", f"Found {len(errors_found)} errors")
                # Write errors to file for the analyzer agent
                self._write_errors_to_file(errors_found)
            else:
                self.update_status("completed", "No errors found in monitoring cycle")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in monitoring: {e}")
            self.update_status("error", f"Monitoring failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _get_recent_log_groups(self):
        """Get recent CloudWatch log groups"""
        self.logger.info("Fetching recent CloudWatch log groups...")
        paginator = self.cloudwatch.get_paginator('describe_log_groups')
        log_groups = []
        
        for page in paginator.paginate(logGroupNamePrefix=self.aws_config.log_group_prefix):
            for group in page['logGroups']:
                log_groups.append(group['logGroupName'])
        
        self.logger.info(f"Found {len(log_groups)} log groups")
        return log_groups
    
    def _search_errors(self, log_group: str):
        """Search for errors in a specific log group"""
        now = int(time.time() * 1000)
        start_time = now - 2 * 60 * 1000  # last 2 minutes
        
        self.logger.debug(f"Searching for errors in log group: {log_group}")
        
        try:
            response = self.cloudwatch.filter_log_events(
                logGroupName=log_group,
                startTime=start_time,
                endTime=now,
            )
            
            errors = []
            for event in response.get("events", []):
                event_id = event.get("eventId")
                if event_id in self.seen_event_ids:
                    continue
                
                self.seen_event_ids.add(event_id)
                msg = event["message"]
                
                if "EXECUTION_STATUS: SUCCESS" not in msg and (
                    "ERROR_CODE: None" not in msg or "ERROR_MESSAGE: None" not in msg
                ):
                    self.logger.error(f"Error detected in {log_group}")
                    error_data = {
                        "log_group": log_group,
                        "event_id": event_id,
                        "timestamp": event["timestamp"],
                        "message": msg
                    }
                    errors.append(error_data)
            
            return errors
            
        except Exception as e:
            self.logger.error(f"Error searching log group {log_group}: {e}")
            return []
    
    def _write_errors_to_file(self, errors):
        """Write errors to file for the analyzer agent to process"""
        try:
            with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                for error in errors:
                    f.write(f"Error in {error['log_group']}:\n{error['message']}\n{'-' * 60}\n")
            self.logger.info(f"Logged {len(errors)} error details to snowflake_errors.log")
        except Exception as e:
            self.logger.error(f"Failed to write errors to file: {e}")
