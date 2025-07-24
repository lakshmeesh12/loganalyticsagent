import os
import json
import time
import logging
import snowflake.connector
import subprocess
from autogen import AssistantAgent
from dotenv import load_dotenv
import re
from datetime import datetime

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
        self.snowflake_conn = None
        try:
            self.snowflake_conn = self._connect_to_snowflake()
        except Exception as e:
            self.logger.error(f"Failed to initialize Snowflake connection: {e}")
            with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] Initialization error: {e}\n{'-' * 60}\n")
        self.analyzer_agent = analyzer_agent

    def _connect_to_snowflake(self):
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

    def receive_error(self, error_message, root_cause, remediation_steps, source, manifest_file=None):
        self.logger.info(f"Received {source} error from analyzer. Root cause: {root_cause}")
        
        if source == "snowflake" and self.snowflake_conn:
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

                    if re.match(r"^\s*GRANT\s+.*\s+ON\s+TABLE\s+.*\s+TO\s+ROLE\s+.*;$", command, flags=re.IGNORECASE):
                        cursor.execute(command)
                        self.logger.info(f"Executed Snowflake command: {command}")
                    else:
                        self.logger.warning(f"Skipped non-GRANT command or unsafe statement: {command}")
                except Exception as e:
                    self.logger.error(f"Failed to execute Snowflake command: {command}, Error: {e}")
            cursor.close()
        elif source == "kubernetes":
            for step in remediation_steps:
                try:
                    if isinstance(step, str) and (step.startswith("kubectl apply -f") or step.startswith("kubectl delete pod")):
                        result = subprocess.run(step, shell=True, capture_output=True, text=True)
                        if result.returncode == 0:
                            self.logger.info(f"Executed Kubernetes command: {step}")
                        else:
                            self.logger.error(f"Failed to execute Kubernetes command: {step}, Error: {result.stderr}")
                            # Log the failure to kubernetes_errors.log to track remediation issues
                            with open("kubernetes_errors.log", "a", encoding="utf-8") as f:
                                f.write(f"Remediation error for {step}:\n{result.stderr}\n{'-' * 60}\n")
                    else:
                        self.logger.warning(f"Skipped invalid Kubernetes command: {step}")
                except Exception as e:
                    self.logger.error(f"Error executing Kubernetes command: {step}, Error: {e}")
                    with open("kubernetes_errors.log", "a", encoding="utf-8") as f:
                        f.write(f"Remediation error for {step}:\n{str(e)}\n{'-' * 60}\n")
        else:
            self.logger.warning(f"Skipping {source} remediation: No Snowflake connection")

    def run(self):
        self.logger.info("FixerAgent is now running...")
        while True:
            if os.path.exists("fix_queue.json") and os.path.getsize("fix_queue.json") > 0:
                try:
                    with open("fix_queue.json", 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    remediation_steps = data.get("remediation_steps", [])
                    source = data.get("source", "snowflake")
                    manifest_file = data.get("manifest_file", None)
                    self.receive_error(
                        data.get("error"),
                        data.get("root_cause"),
                        remediation_steps,
                        source,
                        manifest_file
                    )
                    open("fix_queue.json", 'w').close()
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse fix_queue.json: {e}")
                    open("fix_queue.json", 'w').close()  # Clear malformed file
                except Exception as e:
                    self.logger.error(f"FixerAgent error: {e}")
            time.sleep(10)