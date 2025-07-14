import os
import logging
import openai
from dotenv import load_dotenv
from autogen import AssistantAgent
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, ForbiddenError

load_dotenv()

class FixerAgent(AssistantAgent):
    def __init__(self, name, llm_config, analyzer_agent=None):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("FixerAgent")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.OPENAI_API_KEY = os.getenv("OPEN_API_KEY")
        self.SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
        self.SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        self.SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        self.SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
        self.SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
        self.SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
        self.analyzer_agent = analyzer_agent  # Reference to AnalyzerAgent for feedback

        openai.api_key = self.OPENAI_API_KEY
        self.client = openai

        self.snowflake_conn = self._connect_to_snowflake()

        if not all([
            self.OPENAI_API_KEY, self.SNOWFLAKE_USER, self.SNOWFLAKE_PASSWORD,
            self.SNOWFLAKE_ACCOUNT, self.SNOWFLAKE_WAREHOUSE, self.SNOWFLAKE_DATABASE,
            self.SNOWFLAKE_SCHEMA, self.SNOWFLAKE_ROLE
        ]):
            raise ValueError("Required environment variables not set")

    def _connect_to_snowflake(self):
        try:
            conn = snowflake.connector.connect(
                user=self.SNOWFLAKE_USER,
                password=self.SNOWFLAKE_PASSWORD,
                account=self.SNOWFLAKE_ACCOUNT,
                warehouse=self.SNOWFLAKE_WAREHOUSE,
                database=self.SNOWFLAKE_DATABASE,
                schema=self.SNOWFLAKE_SCHEMA,
                role=self.SNOWFLAKE_ROLE
            )
            self.logger.info("FixerAgent successfully connected to Snowflake")
            return conn
        except Exception as e:
            self.logger.error(f"FixerAgent failed to connect to Snowflake: {e}")
            raise

    def generate_sql_fix(self, root_cause: str, remediation_steps: list) -> list:
        prompt = (
            "You are a Snowflake SQL expert. Given the following root cause and remediation steps for a permission-related error, "
            "generate a list of executable SQL commands to fix the issue. Ensure the commands are safe, precise, and applicable to Snowflake's permission model. "
            "Only generate GRANT or related permission commands, and avoid creating or modifying objects unless explicitly required.\n\n"
            f"Root Cause: {root_cause}\n"
            f"Remediation Steps: {'; '.join(remediation_steps)}\n\n"
            "Return a JSON object with a 'sql_commands' key containing a list of SQL commands."
        )
        try:
            response = self.client.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500
            )
            result = eval(response.choices[0].message.content.strip())
            return result.get("sql_commands", [])
        except Exception as e:
            self.logger.error(f"OpenAI API error while generating SQL: {e}")
            return []

    def execute_fix(self, sql_commands: list) -> tuple[bool, str]:
        try:
            cursor = self.snowflake_conn.cursor()
            for cmd in sql_commands:
                try:
                    cursor.execute(cmd)
                    self.logger.info(f"Executed SQL command: {cmd}")
                except ProgrammingError as pe:
                    self.logger.error(f"Failed to execute SQL command '{cmd}': {pe}")
                    cursor.close()
                    return False, f"SQL execution failed: {pe}"
            cursor.close()
            return True, "All SQL commands executed successfully"
        except ForbiddenError as fe:
            self.logger.error(f"Permission error while applying fix: {fe}")
            return False, f"Permission error: {fe}"
        except Exception as e:
            self.logger.error(f"Unexpected error while applying fix: {e}")
            return False, f"Unexpected error: {e}"

    def verify_fix(self, error_message: str) -> tuple[bool, str]:
        try:
            cursor = self.snowflake_conn.cursor()
            import re
            match = re.search(r"Object '(\w+\.\w+\.\w+)' does not exist or not authorized", error_message)
            if match:
                object_name = match.group(1)
                cursor.execute(f"SELECT 1 FROM {object_name} LIMIT 1")
            else:
                cursor.execute("SELECT CURRENT_TIMESTAMP()")
            cursor.close()
            self.logger.info("Verification query executed successfully")
            return True, "Verification successful"
        except ProgrammingError as pe:
            self.logger.error(f"Verification failed: {pe}")
            return False, f"Verification failed: {pe}"
        except Exception as e:
            self.logger.error(f"Unexpected error during verification: {e}")
            return False, f"Unexpected error: {e}"

    def receive_error(self, error_message: str, root_cause: str, remediation_steps: list):
        self.logger.info(f"Received error for fixing: {error_message[:100]}...")
        sql_commands = self.generate_sql_fix(root_cause, remediation_steps)
        if not sql_commands:
            self.logger.error("No SQL commands generated for remediation")
            with open("remediation.log", 'a', encoding='utf-8') as rem_file:
                rem_file.write(
                    f"Error:\n{error_message}\n"
                    f"Root Cause:\n{root_cause}\n"
                    f"Remediation Steps:\n{'; '.join(remediation_steps)}\n"
                    f"Status: Failed (No SQL commands generated)\n{'='*60}\n"
                )
            return

        success, message = self.execute_fix(sql_commands)
        if success:
            verified, verify_message = self.verify_fix(error_message)
            status = "Fixed" if verified else "Failed Verification"
            with open("remediation.log", 'a', encoding='utf-8') as rem_file:
                rem_file.write(
                    f"Error:\n{error_message}\n"
                    f"Root Cause:\n{root_cause}\n"
                    f"Remediation Steps:\n{'; '.join(remediation_steps)}\n"
                    f"SQL Commands Executed:\n{'; '.join(sql_commands)}\n"
                    f"Status: {status}\n"
                    f"Verification Message: {verify_message}\n{'='*60}\n"
                )
            if not verified and self.analyzer_agent:
                self.analyzer_agent.receive_feedback(error_message, verify_message)
        else:
            with open("remediation.log", 'a', encoding='utf-8') as rem_file:
                rem_file.write(
                    f"Error:\n{error_message}\n"
                    f"Root Cause:\n{root_cause}\n"
                    f"Remediation Steps:\n{'; '.join(remediation_steps)}\n"
                    f"SQL Commands Attempted:\n{'; '.join(sql_commands)}\n"
                    f"Status: Failed Remediation\n"
                    f"Failure Message: {message}\n{'='*60}\n"
                )
            if self.analyzer_agent:
                self.analyzer_agent.receive_feedback(error_message, message)

    def run(self):
        self.logger.info("FixerAgent started, waiting for errors from AnalyzerAgent...")
