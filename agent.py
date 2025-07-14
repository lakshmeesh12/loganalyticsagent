import os
import time
import logging
import openai
from dotenv import load_dotenv
from autogen import AssistantAgent
import snowflake.connector

load_dotenv()

class AnalyzerAgent(AssistantAgent):
    def __init__(self, name, llm_config, fixer_agent=None):
        super().__init__(name=name, llm_config=llm_config)
        self.logger = logging.getLogger("AnalyzerAgent")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.OPENAI_API_KEY = os.getenv("OPEN_API_KEY")
        self.ERROR_LOG = "snowflake_errors.log"
        self.last_position = 0
        self.fixer_agent = fixer_agent  # Reference to FixerAgent for communication

        if not self.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not set")

        openai.api_key = self.OPENAI_API_KEY

    def analyze_error(self, error_message: str, feedback: str = None) -> dict:
        """Analyze error and identify root cause using OpenAI."""
        prompt = (
            "You are a Snowflake error analysis assistant. Given the following error log entry, "
            "analyze the root cause, focusing on permission-related issues, and provide actionable remediation steps. "
            "If feedback from a previous fix attempt is provided, refine the analysis to address why the fix failed.\n\n"
            f"Error: {error_message}\n\n"
            f"Feedback (if any): {feedback or 'None'}\n\n"
            "Return a JSON object with 'root_cause' (a string describing the cause) and 'remediation_steps' (a list of actionable steps)."
        )
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500
            )
            return eval(response.choices[0].message.content.strip())
        except Exception as e:
            self.logger.error(f"OpenAI API error: {e}")
            return {"root_cause": "Failed to analyze error", "remediation_steps": [f"Error: {e}"]}

    def run(self):
        self.logger.info("Starting AnalyzerAgent for error analysis...")
        if not os.path.exists(self.ERROR_LOG):
            open(self.ERROR_LOG, 'a').close()
        try:
            while True:
                with open(self.ERROR_LOG, 'r', encoding='utf-8') as f:
                    f.seek(self.last_position)
                    new_entries = f.read()
                    self.last_position = f.tell()
                if new_entries:
                    errors = new_entries.strip().split('-' * 60)
                    for err in errors:
                        err = err.strip()
                        if not err:
                            continue
                        self.logger.info("New error detected, analyzing...")
                        analysis = self.analyze_error(err)
                        root_cause = analysis.get("root_cause", "Unknown")
                        remediation_steps = analysis.get("remediation_steps", [])

                        # Log the analysis
                        with open("remediation.log", 'a', encoding='utf-8') as rem_file:
                            rem_file.write(
                                f"Error:\n{err}\n"
                                f"Root Cause:\n{root_cause}\n"
                                f"Remediation Steps:\n{'; '.join(remediation_steps)}\n"
                                f"Status: Sent to FixerAgent\n{'='*60}\n"
                            )
                        self.logger.info("Analysis sent to FixerAgent")

                        if self.fixer_agent:
                            self.fixer_agent.receive_error(err, root_cause, remediation_steps)

                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.info("AnalyzerAgent stopped by user")

    def receive_feedback(self, error_message: str, feedback: str):
        """Receive feedback from FixerAgent and re-analyze the error."""
        self.logger.info(f"Received feedback for error: {feedback}")
        analysis = self.analyze_error(error_message, feedback)
        root_cause = analysis.get("root_cause", "Unknown")
        remediation_steps = analysis.get("remediation_steps", [])

        # Log the re-analysis
        with open("remediation.log", 'a', encoding='utf-8') as rem_file:
            rem_file.write(
                f"Error (Re-analyzed):\n{error_message}\n"
                f"Feedback:\n{feedback}\n"
                f"Root Cause (Updated):\n{root_cause}\n"
                f"Remediation Steps (Updated):\n{'; '.join(remediation_steps)}\n"
                f"Status: Sent to FixerAgent\n{'='*60}\n"
            )
        self.logger.info("Re-analysis sent to FixerAgent")

        if self.fixer_agent:
            self.fixer_agent.receive_error(error_message, root_cause, remediation_steps)
