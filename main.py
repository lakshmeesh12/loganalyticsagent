# main.py
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

# Define LLM Config using config_list
llm_config = {
    "config_list": [
        {
            "model": "gpt-4o",
            "api_key": os.getenv("OPEN_API_KEY"),
            "base_url": "https://api.openai.com/v1"
        }
    ]
}

# Initialize all agents
error_analyzer = ErrorAnalyzerAgent(name="ErrorAnalyzerAgent", llm_config=llm_config)
fixer_agent = FixerAgent(name="FixerAgent", llm_config=llm_config, analyzer_agent=error_analyzer)
monitor_agent = MonitorAgent(name="MonitorAgent", llm_config=llm_config, analyzer_agent=error_analyzer)
forwarder_agent = LogForwarderAgent(name="LogForwarderAgent", llm_config=llm_config)

# Link analyzer to fixer
error_analyzer.fixer_agent = fixer_agent

# User proxy for interaction
user_proxy = UserProxyAgent(
    name="Supervisor",
    code_execution_config={"use_docker": False},
    human_input_mode="NEVER"
)

# Group chat setup
group_chat = GroupChat(agents=[user_proxy, error_analyzer, fixer_agent], messages=[], max_round=5)
chat_manager = GroupChatManager(groupchat=group_chat, llm_config=llm_config)

@app.get("/start-logging")
def start_logging():
    user_proxy.initiate_chat(
        chat_manager,
        message="Start analyzing Snowflake logs and fixing issues."
    )
    return {"status": "Chat initiated"}

@app.on_event("startup")
def startup_event():
    # Start forwarder and monitor in background threads
    threading.Thread(target=forwarder_agent.run, daemon=True).start()
    threading.Thread(target=monitor_agent.run, daemon=True).start()
