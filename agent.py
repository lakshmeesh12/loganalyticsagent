# agent.py
import os
import time
import json
import logging
import re
from dotenv import load_dotenv
from autogen import AssistantAgent
from openai import OpenAI
import snowflake.connector

load_dotenv()

class ErrorAnalyzerAgent(AssistantAgent):
    def __init__(self, name, llm_config):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("ErrorAnalyzer")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.OPENAI_API_KEY = os.getenv("OPEN_API_KEY")
        self.client = OpenAI(api_key=self.OPENAI_API_KEY)

        self.snowflake_conn = self._connect_to_snowflake()
        self.ERROR_LOG = "snowflake_errors.log"
        self.last_position = 0

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
            self.logger.info("Connected to Snowflake")
            return conn
        except Exception as e:
            self.logger.error(f"Snowflake connection failed: {e}")
            raise

    def analyze_error(self, error_message: str) -> dict:
        prompt = (
            "You are a Snowflake error analysis assistant. Given the following error log entry, "
            "analyze the root cause (focusing on permission-related issues) and provide actionable remediation steps.\n\n"
            f"Error: {error_message}\n\n"
            "Return a JSON object with:\n"
            "- 'root_cause': a brief description of the issue\n"
            "- 'remediation_steps': a list of valid SQL commands only (e.g., GRANT, CREATE ROLE, SHOW GRANTS, etc.). "
            "Do not include any explanation or additional text. Only pure SQL strings."
        )
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500
            )
            reply_content = response.choices[0].message.content.strip()
            self.logger.info(f"LLM reply: {reply_content}")

            # Extract JSON using regex from markdown-style reply
            match = re.search(r"```json\s*(\{.*?\})\s*```", reply_content, re.DOTALL)
            if match:
                json_str = match.group(1)
            else:
                self.logger.warning("No fenced JSON block found, attempting direct JSON parse.")
                json_str = reply_content  # fallback attempt

            return json.loads(json_str)

        except json.JSONDecodeError as json_err:
            self.logger.error(f"JSON parsing error: {json_err}")
            return {
                "root_cause": "Failed to parse JSON from LLM",
                "remediation_steps": [reply_content]
            }
        except Exception as e:
            self.logger.error(f"OpenAI API error: {e}")
            return {"root_cause": "Failed to analyze", "remediation_steps": [f"Error: {e}"]}

    def run(self):
        self.logger.info("ErrorAnalyzerAgent started")
        if not os.path.exists(self.ERROR_LOG):
            open(self.ERROR_LOG, 'a').close()

        while True:
            with open(self.ERROR_LOG, 'r', encoding='utf-8') as f:
                f.seek(self.last_position)
                new_entries = f.read()
                self.last_position = f.tell()
            if new_entries:
                errors = new_entries.strip().split('-' * 60)
                for err in errors:
                    if not err.strip():
                        continue
                    self.logger.info("Analyzing new error...")
                    result = self.analyze_error(err.strip())
                    output = {
                        "error": err.strip(),
                        "root_cause": result.get("root_cause", "Unknown"),
                        "remediation_steps": result.get("remediation_steps", [])
                    }
                    with open("fix_queue.json", 'w', encoding='utf-8') as out:
                        json.dump(output, out, indent=4)
                    self.logger.info("Analysis written to fix_queue.json")
            time.sleep(10)
