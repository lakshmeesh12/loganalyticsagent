import boto3
import subprocess
import time
import logging
from datetime import datetime, timezone

# AWS + CloudWatch Setup
LOG_GROUP_NAME = "/kubernetes/system"
REGION_NAME = "us-east-1"
cloudwatch = boto3.client('logs', region_name=REGION_NAME)
sequence_tokens = {}

# Kubernetes
NAMESPACE = "default"

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger()

# Ensure log group exists
def ensure_log_group():
    groups = cloudwatch.describe_log_groups(logGroupNamePrefix=LOG_GROUP_NAME)
    if not any(g['logGroupName'] == LOG_GROUP_NAME for g in groups.get("logGroups", [])):
        cloudwatch.create_log_group(logGroupName=LOG_GROUP_NAME)
        logger.info(f"Created log group: {LOG_GROUP_NAME}")

# Create log stream per pod
def ensure_log_stream(stream_name):
    try:
        cloudwatch.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=stream_name)
        logger.debug(f"Created log stream: {stream_name}")
    except cloudwatch.exceptions.ResourceAlreadyExistsException:
        pass

# Send logs to CloudWatch
def send_logs(stream_name, log_lines):
    if not log_lines:
        return

    log_lines = [line for line in log_lines if line.strip()]  # Skip empty lines
    if not log_lines:
        return

    timestamp = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    events = [{"timestamp": timestamp, "message": line} for line in log_lines]

    for i in range(0, len(events), 10000):
        batch = events[i:i + 10000]
        kwargs = {
            "logGroupName": LOG_GROUP_NAME,
            "logStreamName": stream_name,
            "logEvents": batch
        }

        if stream_name in sequence_tokens:
            kwargs["sequenceToken"] = sequence_tokens[stream_name]

        try:
            response = cloudwatch.put_log_events(**kwargs)
            sequence_tokens[stream_name] = response["nextSequenceToken"]
            logger.info(f"Sent {len(batch)} logs to {stream_name}")
        except cloudwatch.exceptions.InvalidSequenceTokenException as e:
            expected = str(e).split("expected sequenceToken is: ")[-1]
            sequence_tokens[stream_name] = expected
            kwargs["sequenceToken"] = expected
            response = cloudwatch.put_log_events(**kwargs)
            sequence_tokens[stream_name] = response["nextSequenceToken"]
            logger.info(f"Retried log batch to {stream_name} after token update")
        except Exception as e:
            logger.error(f"Failed to send logs: {e}")

# Get list of pods
def get_pods():
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", NAMESPACE, "-o", "jsonpath={.items[*].metadata.name}"],
        capture_output=True,
        text=True
    )
    return result.stdout.strip().split()

# Get logs from pod
def get_pod_logs(pod_name):
    result = subprocess.run(
        ["kubectl", "logs", pod_name, "-n", NAMESPACE],
        capture_output=True,
        text=True
    )
    return result.stdout.splitlines()

# Main function
def main():
    ensure_log_group()
    logger.info("Starting Kubernetes log forwarder...")

    last_seen_logs = {}

    while True:
        pods = get_pods()
        for pod in pods:
            stream_name = f"{pod}-{NAMESPACE}"
            ensure_log_stream(stream_name)

            log_lines = get_pod_logs(pod)
            new_logs = []

            if pod not in last_seen_logs:
                # On first run, skip all old logs
                last_seen_logs[pod] = len(log_lines)
                continue

            # Only pick new logs since last run
            prev_line_count = last_seen_logs[pod]
            if len(log_lines) > prev_line_count:
                new_logs = log_lines[prev_line_count:]

            last_seen_logs[pod] = len(log_lines)

            if new_logs:
                send_logs(stream_name, new_logs)

        time.sleep(10)

if __name__ == "__main__":
    main()
