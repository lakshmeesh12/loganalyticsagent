import boto3
import time
import logging
import json
from autogen import AssistantAgent
import os
from datetime import datetime
from dotenv import load_dotenv
from agent import ErrorAnalyzerAgent

load_dotenv()

class MonitorAgent(AssistantAgent):
    def __init__(self, name, llm_config, analyzer_agent=None):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("MONITOR")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.SNOWFLAKE_REGION = 'us-east-1'
        self.SNOWFLAKE_LOG_GROUP_PREFIX = "/snowflake/"
        self.snowflake_cloudwatch = boto3.client("logs", region_name=self.SNOWFLAKE_REGION)
        self.KUBERNETES_REGION = 'ap-south-1'
        self.KUBERNETES_LOG_GROUP = "/aws/eks/crash-fix-cluster/cluster"
        self.kubernetes_cloudwatch = boto3.client("logs", region_name=self.KUBERNETES_REGION)
        self.seen_event_ids = set()
        self.analyzer_agent = analyzer_agent
        self.snowflake_enabled = self.analyzer_agent.snowflake_conn is not None if analyzer_agent else False
        self.recent_pods = {}  # Track recently processed pods with timestamps

    def get_recent_log_groups(self, cloudwatch_client, log_group_prefix=None):
        """Fetch recent CloudWatch log groups for a given client and prefix."""
        self.logger.info(f"Fetching recent CloudWatch log groups for prefix: {log_group_prefix or 'single group'}...")
        log_groups = []
        try:
            if log_group_prefix:
                paginator = cloudwatch_client.get_paginator('describe_log_groups')
                for page in paginator.paginate(logGroupNamePrefix=log_group_prefix):
                    for group in page['logGroups']:
                        log_groups.append(group['logGroupName'])
            else:
                log_groups = [self.KUBERNETES_LOG_GROUP]
            self.logger.info(f"Found {len(log_groups)} log groups")
            return log_groups
        except Exception as e:
            self.logger.error(f"Error fetching log groups: {e}")
            return []

    def search_snowflake_errors(self, log_group):
        """Search for Snowflake errors in a log group."""
        if not self.snowflake_enabled:
            self.logger.warning(f"Skipping Snowflake log monitoring: No Snowflake connection")
            return
        now = int(time.time() * 1000)
        start_time = now - 5 * 1000  # Last 5 seconds for near real-time
        self.logger.debug(f"Searching for Snowflake errors in log group: {log_group}")

        try:
            response = self.snowflake_cloudwatch.filter_log_events(
                logGroupName=log_group,
                startTime=start_time,
                endTime=now,
            )
            for event in response.get("events", []):
                event_id = event.get("eventId")
                if event_id in self.seen_event_ids:
                    continue
                self.seen_event_ids.add(event_id)
                msg = event["message"]

                if "EXECUTION_STATUS: SUCCESS" not in msg and (
                    "ERROR_CODE: None" not in msg or "ERROR_MESSAGE: None" not in msg
                ):
                    self.logger.error(f"Snowflake error detected in {log_group}")
                    error_message = f"Snowflake Error in {log_group}:\n{msg}\n"

                    try:
                        with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                            f.write(error_message + '-' * 60 + "\n")
                        self.logger.info("Logged Snowflake error to snowflake_errors.log")
                    except Exception as e:
                        self.logger.error(f"Failed to write Snowflake error to file: {e}")

                    if self.analyzer_agent:
                        self.analyzer_agent.logger.info("AnalyzerAgent triggered for Snowflake error")
                        self.analyzer_agent.analyze_error(error_message, source="snowflake")
        except Exception as e:
            self.logger.error(f"Error searching Snowflake logs: {e}")

    def search_kubernetes_errors(self, log_group):
        """Search for Kubernetes OOMKilled errors in a log group."""
        now = int(time.time() * 1000)
        start_time = now - 5 * 1000  # Last 5 seconds for near real-time
        self.logger.debug(f"Searching for Kubernetes errors in log group: {log_group}")

        try:
            response = self.kubernetes_cloudwatch.filter_log_events(
                logGroupName=log_group,
                startTime=start_time,
                endTime=now,
                filterPattern="OOMKilled"
            )
            for event in response.get("events", []):
                event_id = event.get("eventId")
                if event_id in self.seen_event_ids:
                    self.logger.info(f"Skipping duplicate event ID: {event_id}")
                    continue
                self.seen_event_ids.add(event_id)
                msg = event["message"]

                if "OOMKilled" in msg:
                    try:
                        event_data = json.loads(msg)
                        if event_data.get("kind") == "Event" and event_data.get("apiVersion") == "audit.k8s.io/v1":
                            if "Pod \"oom-test\" is invalid" in msg or "Forbidden" in msg:
                                self.logger.info(f"Skipping audit log for kubectl apply failure: {msg[:100]}...")
                                continue

                        namespace = event_data.get("objectRef", {}).get("namespace")
                        pod_name = event_data.get("objectRef", {}).get("name")
                        container_statuses = event_data.get("requestObject", {}).get("status", {}).get("containerStatuses", [])
                        for status in container_statuses:
                            if status.get("lastState", {}).get("terminated", {}).get("reason") == "OOMKilled":
                                container_name = status.get("name")
                                pod_key = f"{namespace}/{pod_name}"
                                current_time = datetime.now()
                                # Skip if pod was processed within the last 1 minute
                                if pod_key in self.recent_pods:
                                    last_processed = self.recent_pods[pod_key]
                                    if (current_time - last_processed).total_seconds() < 60:
                                        self.logger.info(f"Skipping recently processed pod {pod_key} within 1-minute window")
                                        continue
                                self.recent_pods[pod_key] = current_time

                                error_message = f"Container {container_name} in pod {namespace}/{pod_name} killed due to OutOfMemory"
                                break
                        else:
                            self.logger.warning(f"No OOMKilled container found in JSON: {msg[:100]}...")
                            continue

                        self.logger.error(f"Kubernetes OOMKilled error detected in {log_group}")
                        with open("kubernetes_errors.log", "a", encoding="utf-8") as f:
                            f.write(error_message + '-' * 60 + "\n")
                        self.logger.info("Logged Kubernetes error to kubernetes_errors.log")

                        if self.analyzer_agent:
                            self.analyzer_agent.logger.info("AnalyzerAgent triggered for Kubernetes error")
                            self.analyzer_agent.analyze_error(error_message, source="kubernetes")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse Kubernetes log JSON: {e}")
                        error_message = f"Kubernetes Error in {log_group}:\n{msg}\n"
                        with open("kubernetes_errors.log", "a", encoding="utf-8") as f:
                            f.write(error_message + '-' * 60 + "\n")
                        self.logger.info("Logged Kubernetes error to kubernetes_errors.log")
                        if self.analyzer_agent:
                            self.analyzer_agent.logger.info("AnalyzerAgent triggered for Kubernetes error")
                            self.analyzer_agent.analyze_error(error_message, source="kubernetes")
                    except Exception as e:
                        self.logger.error(f"Failed to write Kubernetes error to file: {e}")
        except Exception as e:
            self.logger.error(f"Error searching Kubernetes logs: {e}")

    def run(self):
        self.logger.info("Starting CloudWatch logs monitoring...")
        try:
            while True:
                if self.snowflake_enabled:
                    snowflake_groups = self.get_recent_log_groups(self.snowflake_cloudwatch, self.SNOWFLAKE_LOG_GROUP_PREFIX)
                    for group in snowflake_groups:
                        self.search_snowflake_errors(group)
                kubernetes_groups = self.get_recent_log_groups(self.kubernetes_cloudwatch)
                for group in kubernetes_groups:
                    self.search_kubernetes_errors(group)
                # Clean up recent_pods older than 1 minute
                current_time = datetime.now()
                self.recent_pods = {
                    k: v for k, v in self.recent_pods.items()
                    if (current_time - v).total_seconds() < 60
                }
                time.sleep(10)  # Reduced polling interval for faster response
        except KeyboardInterrupt:
            self.logger.info("Monitoring stopped by user")