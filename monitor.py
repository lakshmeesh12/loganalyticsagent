# monitor.py
import boto3
import time
import logging
from autogen import AssistantAgent
import os
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
        self.REGION = 'us-east-1'
        self.LOG_GROUP_PREFIX = "/snowflake/"
        self.cloudwatch = boto3.client("logs", region_name=self.REGION)
        self.seen_event_ids = set()
        self.analyzer_agent = analyzer_agent

    def get_recent_log_groups(self):
        self.logger.info("Fetching recent CloudWatch log groups...")
        paginator = self.cloudwatch.get_paginator('describe_log_groups')
        log_groups = []
        for page in paginator.paginate(logGroupNamePrefix=self.LOG_GROUP_PREFIX):
            for group in page['logGroups']:
                log_groups.append(group['logGroupName'])
        self.logger.info(f"Found {len(log_groups)} log groups")
        return log_groups

    def search_errors(self, log_group):
        now = int(time.time() * 1000)
        start_time = now - 2 * 60 * 1000  # last 2 minutes
        self.logger.debug(f"Searching for errors in log group: {log_group}")

        response = self.cloudwatch.filter_log_events(
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
                self.logger.error(f"Error detected in {log_group}")
                error_message = f"Error in {log_group}:\n{msg}\n"

                try:
                    with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                        f.write(error_message + '-' * 60 + "\n")
                    self.logger.info("Logged error details to snowflake_errors.log")
                except Exception as e:
                    self.logger.error(f"Failed to write error to file: {e}")

                if self.analyzer_agent:
                    self.analyzer_agent.logger.info("AnalyzerAgent triggered from MonitorAgent")
                    analysis = self.analyzer_agent.analyze_error(error_message)
                    root_cause = analysis.get("root_cause", "Unknown")
                    remediation_steps = analysis.get("remediation_steps", [])
                    if self.analyzer_agent.fixer_agent:
                        self.analyzer_agent.fixer_agent.receive_error(
                            error_message, root_cause, remediation_steps
                        )

    def run(self):
        self.logger.info("Starting CloudWatch logs monitoring...")
        try:
            while True:
                groups = self.get_recent_log_groups()
                for group in groups:
                    self.search_errors(group)
                time.sleep(15)
        except KeyboardInterrupt:
            self.logger.info("Monitoring stopped by user")

if __name__ == "__main__":
    from agent import ErrorAnalyzerAgent
    from fixer import FixerAgent

    config_list = [{"model": "gpt-4o", "api_key": os.getenv("OPEN_API_KEY")}]
    analyzer = ErrorAnalyzerAgent(name="AnalyzerAgent", llm_config={"config_list": config_list})
    fixer = FixerAgent(name="FixerAgent", llm_config={"config_list": config_list}, analyzer_agent=analyzer)
    analyzer.fixer_agent = fixer

    agent = MonitorAgent(
        name="MonitorAgent",
        llm_config={"config_list": config_list},
        analyzer_agent=analyzer
    )
    agent.run()
