# agents/analyzer_agent.py
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any
import openai

from .base_agent import BaseAgent

class AnalyzerAgent(BaseAgent):
    """Agent responsible for analyzing errors and providing remediation"""
    
    def __init__(self):
        super().__init__("Analyzer", "ANALYZER")
        self.client = None
        self.error_log_file = "snowflake_errors.log"
        self.remediation_log_file = "remediation.log"
        self.last_position = 0
        
    def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize the analyzer with OpenAI configuration"""
        try:
            api_key = config.get("openai_api_key") or os.getenv("OPENAI_API_KEY")

            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            openai.api_key = api_key

            
            # Ensure error log file exists
            if not os.path.exists(self.error_log_file):
                open(self.error_log_file, 'a').close()
            
            self.update_status("initialized", "Analyzer agent initialized successfully")
            self.logger.info("Analyzer agent initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize analyzer: {e}")
            self.update_status("error", f"Initialization failed: {e}")
            return False
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute error analysis task"""
        try:
            self.update_status("running", "Analyzing errors and generating remediations")
            
            new_errors = self._read_new_errors()
            remediations_generated = 0
            
            if new_errors:
                errors = new_errors.strip().split('-' * 60)
                for error in errors:
                    error = error.strip()
                    if not error:
                        continue
                    
                    self.logger.info("New error detected, sending to AI agent...")
                    remediation = self._analyze_error(error)
                    self._write_remediation(error, remediation)
                    remediations_generated += 1
            
            result = {
                "status": "success",
                "remediations_generated": remediations_generated,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if remediations_generated > 0:
                self.update_status("completed", f"Generated {remediations_generated} remediations")
            else:
                self.update_status("completed", "No new errors to analyze")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in analysis: {e}")
            self.update_status("error", f"Analysis failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _read_new_errors(self) -> str:
        """Read new errors from the error log file"""
        try:
            with open(self.error_log_file, 'r', encoding='utf-8') as f:
                f.seek(self.last_position)
                new_entries = f.read()
                self.last_position = f.tell()
            return new_entries
        except Exception as e:
            self.logger.error(f"Error reading error log file: {e}")
            return ""
    
    def _analyze_error(self, message: str) -> str:
        """Use OpenAI to analyze the error message and provide remediation steps"""
        prompt = (
            "You are a CloudWatch error analysis assistant. "
            "Given the following error log entry, analyze the root cause and provide clear, actionable remediation steps:\n\n"
            f"{message}\n\n"
        )
        
        try:
            from openai import OpenAI  # ✅ Add this import at the top

            # Inside _analyze_error(self, message: str)
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))  # ✅ Use your API key

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant specialized in analyzing system errors and providing remediation steps."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.1
            )

            return response.choices[0].message.content.strip()

        except Exception as e:
            self.logger.error(f"OpenAI API error: {e}")
            return f"Failed to analyze error: {e}"
    
    def _write_remediation(self, error: str, remediation: str):
        """Write error and remediation to the remediation log file"""
        try:
            with open(self.remediation_log_file, 'a', encoding='utf-8') as f:
                f.write(f"Error:\n{error}\nRemediation:\n{remediation}\n{'='*60}\n")
            self.logger.info("Remediation written to remediation.log")
            print(f"\nRemediation for error:\n{remediation}\n{'='*60}", flush=True)
        except Exception as e:
            self.logger.error(f"Failed to write remediation: {e}")