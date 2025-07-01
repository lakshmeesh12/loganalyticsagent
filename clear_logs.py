import boto3

# === AWS CloudWatch Client ===
logs_client = boto3.client('logs')

# === Log Groups to Clean ===
log_groups = [
    "/snowflake/query_history",
    "/snowflake/login_history",
    "/snowflake/access_history",
    "/snowflake/grants_to_users",
    "/snowflake/data_transfer_history",
    "/snowflake/stages"
]

def delete_all_log_streams(log_group):
    print(f"\n🧹 Cleaning log group: {log_group}")
    paginator = logs_client.get_paginator('describe_log_streams')
    page_iterator = paginator.paginate(logGroupName=log_group)

    found = False
    for page in page_iterator:
        for stream in page['logStreams']:
            stream_name = stream['logStreamName']
            print(f"🔻 Deleting log stream: {stream_name}")
            logs_client.delete_log_stream(logGroupName=log_group, logStreamName=stream_name)
            found = True

    if not found:
        print("✅ No log streams to delete.")

if __name__ == "__main__":
    for group in log_groups:
        try:
            delete_all_log_streams(group)
        except logs_client.exceptions.ResourceNotFoundException:
            print(f"⚠️ Log group not found: {group}")
