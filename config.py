# config.py
import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class SnowflakeConfig:
    user: str = 'keerthana'
    password: str = 'Quadrantkeerthana2025'
    account: str = 'pcc86913.us-east-1'
    role: str = 'ACCOUNTADMIN'
    warehouse: str = 'COMPUTE_WH'
    database: str = 'SNOWFLAKE'
    schema: str = 'ACCOUNT_USAGE'

@dataclass
class AWSConfig:
    region: str = 'us-east-1'
    log_group_prefix: str = "/snowflake/"

@dataclass
class AutoGenConfig:
    api_key: str = os.getenv("OPENAI_API_KEY", "")

    model: str = "gpt-4"
    temperature: float = 0.1

# Log configuration for different Snowflake views
LOG_CONFIG = {
    "QUERY_HISTORY": "START_TIME",
    "LOGIN_HISTORY": "EVENT_TIMESTAMP", 
    "TASK_HISTORY": "SCHEDULED_TIME",
    "GRANTS_TO_USERS": "CREATED_ON",
    "WAREHOUSE_LOAD_HISTORY": "START_TIME"
}