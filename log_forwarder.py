import snowflake.connector
import boto3
import time
from datetime import datetime, timedelta, timezone
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("FORWARDER")


# --- Snowflake Credentials ---
USER = 'keerthana'
PASSWORD = 'Quadrantkeerthana2025'
ACCOUNT = 'pcc86913.us-east-1'
ROLE = 'ACCOUNTADMIN'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'SNOWFLAKE'
SCHEMA = 'ACCOUNT_USAGE'

# --- AWS CloudWatch Client ---
cloudwatch = boto3.client('logs', region_name='us-east-1')




# --- Log types and metadata ---
LOG_CONFIG = {
    "QUERY_HISTORY": "START_TIME",
    "LOGIN_HISTORY": "EVENT_TIMESTAMP",
    "TASK_HISTORY": "SCHEDULED_TIME",
    "GRANTS_TO_USERS": "CREATED_ON",
    "WAREHOUSE_LOAD_HISTORY": "START_TIME"
}

# --- Initialize per-view last seen timestamps ---
last_timestamps = {
    view: datetime.now(timezone.utc) - timedelta(minutes=5)
    for view in LOG_CONFIG
}
# Ensure log group exists in CloudWatch
def ensure_log_group_exists(log_group):
    logger.debug(f"Checking if log group {log_group} exists")
    groups = cloudwatch.describe_log_groups(logGroupNamePrefix=log_group)
    if not any(group['logGroupName'] == log_group for group in groups.get('logGroups', [])):
        cloudwatch.create_log_group(logGroupName=log_group)
        logger.info(f"Created log group: {log_group}")

# Create a log stream if it doesn't exist (per run/session)
def create_log_stream(log_group, stream_name):
    try:
        cloudwatch.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
        logger.debug(f"Created log stream: {log_group}/{stream_name}")
    except cloudwatch.exceptions.ResourceAlreadyExistsException:
        logger.debug(f"Log stream already exists: {log_group}/{stream_name}")

# Send log events to CloudWatch
def send_log_event(log_group, log_stream, message):
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    global sequence_token
    kwargs = {
        'logGroupName': log_group,
        'logStreamName': log_stream,
        'logEvents': [{
            'timestamp': timestamp_ms,
            'message': message
        }]
    }
    try:
        if log_stream in sequence_token:
            kwargs['sequenceToken'] = sequence_token[log_stream]
        response = cloudwatch.put_log_events(**kwargs)
        sequence_token[log_stream] = response['nextSequenceToken']
        logger.info(f"Sent log to {log_group}/{log_stream}")
    except cloudwatch.exceptions.InvalidSequenceTokenException as e:
        expected = str(e).split("expected sequenceToken is: ")[-1]
        sequence_token[log_stream] = expected
        kwargs['sequenceToken'] = expected
        response = cloudwatch.put_log_events(**kwargs)
        sequence_token[log_stream] = response['nextSequenceToken']
        logger.info(f"Retried log to {log_group}/{log_stream} after token fix")
    except Exception as e:
        logger.error(f"Failed to send log to {log_group}/{log_stream}: {e}")

def fetch_and_forward_logs(conn, view_name, timestamp_col):
    global last_timestamp
    log_group = f"/snowflake/{view_name}"
    log_stream = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")

    ensure_log_group_exists(log_group)
    create_log_stream(log_group, log_stream)

    cursor = conn.cursor()
    try:
        query = f"""
        SELECT *
        FROM {DATABASE}.{SCHEMA}.{view_name}
        WHERE {timestamp_col} > %s
        AND {timestamp_col} IS NOT NULL
        ORDER BY {timestamp_col} ASC
        LIMIT 100;
    """



        cursor.execute(query, (last_timestamps[view_name],))
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        if not rows:
            logger.info(f"No new logs for {view_name}")
        else:
            logger.info(f"Found {len(rows)} new logs for {view_name}")
            for row in rows:
                message = f"{view_name} LOG ENTRY\n"
                for col, val in zip(columns, row):
                    message += f"{col}: {val}\n"
                send_log_event(log_group, log_stream, message)

            last_timestamps[view_name] = rows[-1][columns.index(timestamp_col)]
    except Exception as e:
        logger.error(f"Error fetching {view_name}: {e}")
        with open("snowflake_errors.log", "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now()}] Error fetching {view_name}:\n{e}\n{'-' * 60}\n")


# Connect to Snowflake
def connect_to_snowflake():
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    logger.info("Connected to Snowflake")
    return conn

# Main loop
sequence_token = {}
def main():
    conn = connect_to_snowflake()
    logger.info("Real-time log forwarding started...")
    try:
        while True:
            for log_type, time_col in LOG_CONFIG.items():
                fetch_and_forward_logs(conn, log_type, time_col)
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Log forwarding stopped by user")
    finally:
        conn.close()
        logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()