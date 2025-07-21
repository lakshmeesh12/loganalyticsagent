# fixer.py
import os
import json
import time
import logging
import snowflake.connector
from autogen import AssistantAgent
from dotenv import load_dotenv
import re
load_dotenv()

class FixerAgent(AssistantAgent):
    def __init__(self, name, llm_config=None, analyzer_agent=None):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("FixerAgent")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.snowflake_conn = self._connect_to_snowflake()
        self.analyzer_agent = analyzer_agent

    def _connect_to_snowflake(self):
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
            self.logger.info("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            raise

    

    def receive_error(self, error_message, root_cause, remediation_steps):
        self.logger.info(f"Received error from analyzer. Root cause: {root_cause}")
        cursor = self.snowflake_conn.cursor()

        for step in remediation_steps:
            try:
                if isinstance(step, dict) and "command" in step:
                    command = step["command"]
                elif isinstance(step, str):
                    command = step
                else:
                    self.logger.warning(f"Skipping unrecognized step format: {step}")
                    continue

                # Only execute if it's a valid GRANT command
                if re.match(r"^\s*GRANT\s+.*\s+ON\s+TABLE\s+.*\s+TO\s+ROLE\s+.*;$", command, flags=re.IGNORECASE):
                    cursor.execute(command)
                    self.logger.info(f"Executed: {command}")
                else:
                    self.logger.warning(f"Skipped non-GRANT command or unsafe statement: {command}")

            except Exception as e:
                self.logger.error(f"Failed to execute: {command}, Error: {e}")

        cursor.close()


    def run(self):
        self.logger.info("FixerAgent is now running...")
        while True:
            if os.path.exists("fix_queue.json"):
                try:
                    with open("fix_queue.json", 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    remediation_steps = data.get("remediation_steps", [])
                    self.receive_error(data.get("error"), data.get("root_cause"), remediation_steps)
                except Exception as e:
                    self.logger.error(f"FixerAgent error: {e}")
            time.sleep(10)
