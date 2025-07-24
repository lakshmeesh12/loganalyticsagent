import os
import time
import json
import logging
import re
import subprocess
import yaml
from datetime import datetime
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
        self.snowflake_conn = None
        try:
            self.snowflake_conn = self._connect_to_snowflake()
        except Exception as e:
            self.logger.error(f"Failed to initialize Snowflake connection: {e}")
            with open("snowflake_errors.log", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] Initialization error: {e}\n{'-' * 60}\n")
        self.ERROR_LOG_SNOWFLAKE = "snowflake_errors.log"
        self.ERROR_LOG_KUBERNETES = "kubernetes_errors.log"
        self.last_position_snowflake = 0
        self.last_position_kubernetes = 0
        self.temp_manifest_dir = "./temp_manifests"
        os.makedirs(self.temp_manifest_dir, exist_ok=True)

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
        self.logger.info("Connected to Snowflake")
        return conn

    def _extract_pod_info(self, error_message: str) -> tuple:
        """Extract pod name, namespace, and container name from OOMKilled error message."""
        # First try the string pattern
        pattern = r"Container\s+([^\s]+)\s+in\s+pod\s+([^\s]+)/([^\s]+)\s+killed\s+due\s+to\s+OutOfMemory"
        match = re.search(pattern, error_message)
        if match:
            container, namespace, pod = match.groups()
            return pod, namespace, container

        # Fallback to JSON audit log parsing
        try:
            json_match = re.search(r'\{.*\}', error_message, re.DOTALL)
            if not json_match:
                self.logger.warning(f"No JSON found in error message: {error_message}")
                return None, None, None

            event_data = json.loads(json_match.group(0))
            namespace = event_data.get("objectRef", {}).get("namespace")
            pod_name = event_data.get("objectRef", {}).get("name")
            container_statuses = event_data.get("requestObject", {}).get("status", {}).get("containerStatuses", [])
            for status in container_statuses:
                if status.get("lastState", {}).get("terminated", {}).get("reason") == "OOMKilled":
                    container_name = status.get("name")
                    return pod_name, namespace, container_name

            self.logger.warning(f"No OOMKilled container found in JSON: {error_message}")
            return None, None, None
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse JSON in error message: {e}")
            return None, None, None

    def _modify_pod_manifest(self, pod_name: str, namespace: str, container_name: str) -> tuple:
        """Fetch pod manifest, modify resource limits for the specific container, and save to a file."""
        try:
            cmd = f"kubectl get pod {pod_name} -n {namespace} -o yaml"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                self.logger.error(f"Failed to fetch pod manifest: {result.stderr}")
                return None, []

            manifest = yaml.safe_load(result.stdout)
            containers = manifest.get('spec', {}).get('containers', [])
            if not containers:
                self.logger.error(f"No containers found in pod {pod_name}")
                return None, []

            for container in containers:
                if container.get('name') == container_name:
                    resources = container.setdefault('resources', {})
                    limits = resources.setdefault('limits', {})
                    requests = resources.setdefault('requests', {})

                    cpu_limit = limits.get('cpu', '500m')
                    memory_limit = limits.get('memory', '512Mi')
                    cpu_request = requests.get('cpu', '250m')
                    memory_request = requests.get('memory', '256Mi')

                    if 'm' in cpu_limit:
                        cpu_val = int(cpu_limit.replace('m', '')) * 2
                        limits['cpu'] = f"{cpu_val}m"
                        requests['cpu'] = f"{cpu_val // 2}m"
                    else:
                        cpu_val = float(cpu_limit) * 2
                        limits['cpu'] = f"{cpu_val}"
                        requests['cpu'] = f"{cpu_val / 2}"

                    if 'Mi' in memory_limit:
                        mem_val = int(memory_limit.replace('Mi', '')) * 2
                        limits['memory'] = f"{mem_val}Mi"
                        requests['memory'] = f"{mem_val // 2}Mi"
                    elif 'Gi' in memory_limit:
                        mem_val = float(memory_limit.replace('Gi', '')) * 2
                        limits['memory'] = f"{mem_val}Gi"
                        requests['memory'] = f"{mem_val / 2}Gi"
                    break
            else:
                self.logger.error(f"Container {container_name} not found in pod {pod_name}")
                return None, []

            # Remove status and metadata fields that can't be applied
            manifest.pop('status', None)
            metadata = manifest.get('metadata', {})
            metadata.pop('creationTimestamp', None)
            metadata.pop('resourceVersion', None)
            metadata.pop('uid', None)
            metadata.pop('generation', None)

            manifest_file = f"{self.temp_manifest_dir}/{namespace}_{pod_name}.yaml"
            with open(manifest_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(manifest, f)

            # Return commands to delete and recreate the pod
            delete_cmd = f"kubectl delete pod {pod_name} -n {namespace}"
            apply_cmd = f"kubectl apply -f {manifest_file}"
            return manifest_file, [delete_cmd, apply_cmd]

        except Exception as e:
            self.logger.error(f"Error modifying pod manifest: {e}")
            return None, []

    def analyze_error(self, error_message: str, source: str) -> dict:
        if source == "snowflake" and self.snowflake_conn:
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

                match = re.search(r"```json\s*(\{.*?\})\s*```", reply_content, re.DOTALL)
                if match:
                    json_str = match.group(1)
                else:
                    self.logger.warning("No fenced JSON block found, attempting direct JSON parse.")
                    json_str = reply_content

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
        elif source == "kubernetes":
            pod_name, namespace, container_name = self._extract_pod_info(error_message)
            if not pod_name or not namespace or not container_name:
                return {
                    "root_cause": "Failed to extract pod information",
                    "remediation_steps": []
                }

            manifest_file, commands = self._modify_pod_manifest(pod_name, namespace, container_name)
            if not commands:
                return {
                    "root_cause": "Failed to modify pod manifest",
                    "remediation_steps": []
                }

            return {
                "root_cause": f"OOMKilled error in pod {pod_name} in namespace {namespace}",
                "remediation_steps": commands,
                "manifest_file": manifest_file
            }
        else:
            return {
                "root_cause": f"Cannot analyze {source} error: No Snowflake connection",
                "remediation_steps": []
            }

    def run(self):
        self.logger.info("ErrorAnalyzerAgent started")
        for log_file in [self.ERROR_LOG_SNOWFLAKE, self.ERROR_LOG_KUBERNETES]:
            if not os.path.exists(log_file):
                open(log_file, 'a').close()

        while True:
            if self.snowflake_conn and os.path.exists(self.ERROR_LOG_SNOWFLAKE):
                with open(self.ERROR_LOG_SNOWFLAKE, 'r', encoding='utf-8') as f:
                    f.seek(self.last_position_snowflake)
                    new_entries = f.read()
                    self.last_position_snowflake = f.tell()
                if new_entries:
                    errors = new_entries.strip().split('-' * 60)
                    for err in errors:
                        if not err.strip():
                            continue
                        self.logger.info("Analyzing new Snowflake error...")
                        result = self.analyze_error(err.strip(), source="snowflake")
                        output = {
                            "error": err.strip(),
                            "root_cause": result.get("root_cause", "Unknown"),
                            "remediation_steps": result.get("remediation_steps", []),
                            "source": "snowflake"
                        }
                        with open("fix_queue.json", 'w', encoding='utf-8') as out:
                            json.dump(output, out, indent=4)
                        self.logger.info("Snowflake analysis written to fix_queue.json")

            if os.path.exists(self.ERROR_LOG_KUBERNETES):
                with open(self.ERROR_LOG_KUBERNETES, 'r', encoding='utf-8') as f:
                    f.seek(self.last_position_kubernetes)
                    new_entries = f.read()
                    self.last_position_kubernetes = f.tell()
                if new_entries:
                    errors = new_entries.strip().split('-' * 60)
                    for err in errors:
                        if not err.strip():
                            continue
                        self.logger.info("Analyzing new Kubernetes error...")
                        result = self.analyze_error(err.strip(), source="kubernetes")
                        output = {
                            "error": err.strip(),
                            "root_cause": result.get("root_cause", "Unknown"),
                            "remediation_steps": result.get("remediation_steps", []),
                            "manifest_file": result.get("manifest_file", None),
                            "source": "kubernetes"
                        }
                        with open("fix_queue.json", 'w', encoding='utf-8') as out:
                            json.dump(output, out, indent=4)
                        self.logger.info("Kubernetes analysis written to fix_queue.json")

            time.sleep(10)