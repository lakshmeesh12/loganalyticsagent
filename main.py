from fastapi import FastAPI
import threading
import sys
import os
from dotenv import load_dotenv
from autogen import AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager
from agent import ErrorAnalyzerAgent
from fixer import FixerAgent
from log_forwarder import LogForwarderAgent
from monitor import MonitorAgent

load_dotenv()

app = FastAPI()

llm_config = {
    "config_list": [
        {
            "model": "gpt-4o",
            "api_key": os.getenv("OPEN_API_KEY"),
            "base_url": "https://api.openai.com/v1"
        }
    ]
}

# Initialize agents with error handling
error_analyzer = None
fixer_agent = None
monitor_agent = None
forwarder_agent = None
try:
    error_analyzer = ErrorAnalyzerAgent(name="ErrorAnalyzerAgent", llm_config=llm_config)
    fixer_agent = FixerAgent(name="FixerAgent", llm_config=llm_config, analyzer_agent=error_analyzer)
    error_analyzer.fixer_agent = fixer_agent
    monitor_agent = MonitorAgent(name="MonitorAgent", llm_config=llm_config, analyzer_agent=error_analyzer)
    forwarder_agent = LogForwarderAgent(name="LogForwarderAgent", llm_config=llm_config)
except Exception as e:
    print(f"Failed to initialize one or more agents: {e}", file=sys.stderr)

user_proxy = UserProxyAgent(
    name="Supervisor",
    code_execution_config={"use_docker": False},
    human_input_mode="NEVER"
)

group_chat = GroupChat(agents=[user_proxy, error_analyzer, fixer_agent], messages=[], max_round=5)
chat_manager = GroupChatManager(groupchat=group_chat, llm_config=llm_config)

@app.get("/start-logging")
def start_logging():
    if user_proxy and chat_manager:
        user_proxy.initiate_chat(
            chat_manager,
            message="Start analyzing logs and fixing issues."
        )
        return {"status": "Chat initiated"}
    return {"status": "Failed to initiate chat due to missing agents"}

@app.on_event("startup")
def startup_event():
    if forwarder_agent and forwarder_agent.conn:
        threading.Thread(target=forwarder_agent.run, daemon=True).start()
    if monitor_agent:
        threading.Thread(target=monitor_agent.run, daemon=True).start()
    if error_analyzer:
        threading.Thread(target=error_analyzer.run, daemon=True).start()
    if fixer_agent:
        threading.Thread(target=fixer_agent.run, daemon=True).start()