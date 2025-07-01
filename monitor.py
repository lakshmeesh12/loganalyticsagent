# monitor.py

import boto3
import time
from datetime import datetime, timedelta, timezone
import logging

seen_event_ids = set()


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("MONITOR")


# Setup
REGION = 'us-east-1'
LOG_GROUP_PREFIX = "/snowflake/"

cloudwatch = boto3.client("logs", region_name=REGION)

def get_recent_log_groups():
    logger.info("Fetching recent CloudWatch log groups...")
    paginator = cloudwatch.get_paginator('describe_log_groups')
    log_groups = []
    for page in paginator.paginate(logGroupNamePrefix=LOG_GROUP_PREFIX):
        for group in page['logGroups']:
            log_groups.append(group['logGroupName'])
    logger.info(f"Found {len(log_groups)} log groups")
    return log_groups

def search_errors(log_group):
    now = int(time.time() * 1000)
    start_time = now - 2 * 60 * 1000  # last 2 minutes

    logger.debug(f"Searching for errors in log group: {log_group}")
    response = cloudwatch.filter_log_events(
        logGroupName=log_group,
        startTime=start_time,
        endTime=now,
    )

    for event in response.get("events", []):
        event_id = event.get("eventId")
        if event_id in seen_event_ids:
            continue  # skip if we've already processed this one
        seen_event_ids.add(event_id)

        msg = event["message"]
        if "EXECUTION_STATUS: SUCCESS" not in msg and (
            "ERROR_CODE: None" not in msg or "ERROR_MESSAGE: None" not in msg
        ):
            logger.error(f"Error detected in {log_group}")
            try:
                with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                    f.write(f"Error in {log_group}:\n{msg}\n{'-' * 60}\n")
                logger.info(f"Logged error details to snowflake_errors.log")
            except Exception as e:
                logger.error(f"Failed to write error to file: {e}")



def monitor_loop():
    logger.info("Starting CloudWatch logs monitoring...")
    try:
        while True:
            groups = get_recent_log_groups()
            for group in groups:
                search_errors(group)
            time.sleep(15)
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")

if __name__ == "__main__":
    monitor_loop()