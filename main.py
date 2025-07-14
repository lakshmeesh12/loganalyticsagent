from fastapi import FastAPI
import threading
import os
from dotenv import load_dotenv
import sys
from autogen import AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager
from log_forwarder import LogForwarderAgent
from monitor import MonitorAgent
from agent import AnalyzerAgent
from fixer import FixerAgent

load_dotenv()
app = FastAPI()

# Setup LLM config
config_list = [{"model": "gpt-4o", "api_key": os.getenv("OPEN_API_KEY")}]

# Agents
supervisor = AssistantAgent(
    name="Supervisor",
    llm_config={"config_list": config_list},
    system_message="You are the supervisor agent. Orchestrate the log forwarder, monitor, analyzer, and fixer agents."
)

user_proxy = UserProxyAgent(
    name="UserProxy",
    human_input_mode="NEVER",
    max_consecutive_auto_reply=0,
    code_execution_config={"work_dir": ".", "use_docker": False}
)

log_forwarder = LogForwarderAgent(name="LogForwarderAgent", llm_config={"config_list": config_list})
analyzer = AnalyzerAgent(name="AnalyzerAgent", llm_config={"config_list": config_list})
fixer = FixerAgent(name="FixerAgent", llm_config={"config_list": config_list}, analyzer_agent=analyzer)
analyzer.fixer_agent = fixer
monitor = MonitorAgent(name="MonitorAgent", llm_config={"config_list": config_list}, analyzer_agent=analyzer)

# GroupChat for coordination
group_chat = GroupChat(
    agents=[supervisor, log_forwarder, monitor, analyzer, fixer],
    messages=[],
    max_round=10
)

manager = GroupChatManager(groupchat=group_chat, llm_config={"config_list": config_list})

@app.on_event("startup")
def startup_event():
    print("🚀 FastAPI started. Visit /start-logging to trigger agents.")

@app.get("/start-logging")
async def start_logging():
    threading.Thread(target=log_forwarder.run, daemon=True).start()
    threading.Thread(target=monitor.run, daemon=True).start()
    threading.Thread(target=analyzer.run, daemon=True).start()
    threading.Thread(target=fixer.run, daemon=True).start()

    user_proxy.initiate_chat(
        manager,
        message="Start the log analysis and remediation workflow: fetch logs from Snowflake, monitor for errors, analyze errors, and apply fixes."
    )
    return {"status": "Agents running and chat coordination initiated."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
