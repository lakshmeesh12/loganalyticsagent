# main.py

import asyncio
from fastapi import FastAPI, BackgroundTasks
from orchestrator.supervisor import SupervisorOrchestrator
import threading
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="Multi-Agent Log Analysis System")
supervisor = SupervisorOrchestrator()
system_thread = None

@app.on_event("startup")  # Changed from @app.on_startup
async def startup_event():
    """Initialize the system on startup"""
    config = {
        "openai_api_key": os.getenv("OPENAI_API_KEY")
    }

    
    if not supervisor.initialize(config):
        print("Failed to initialize supervisor")

def run_system():
    """Run the system in a separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor.start_system())

@app.post("/start-system")
async def start_system():
    """Start the multi-agent system"""
    global system_thread
    
    if system_thread and system_thread.is_alive():
        return {"status": "System is already running"}
    
    system_thread = threading.Thread(target=run_system, daemon=True)
    system_thread.start()
    
    return {"status": "Multi-Agent Log Analysis System started successfully"}

@app.post("/stop-system")
async def stop_system():
    """Stop the multi-agent system"""
    supervisor.stop_system()
    return {"status": "Multi-Agent Log Analysis System stopped"}

@app.get("/system-status")
async def get_system_status():
    """Get current system status"""
    return supervisor.get_system_status()

@app.post("/add-agent")
async def add_agent(agent_name: str, agent_config: dict = None):
    """Add a new agent to the system (for extensibility)"""
    # This would need to be implemented based on the specific agent type
    return {"status": f"Agent addition endpoint ready for {agent_name}"}

@app.get("/agents/{agent_name}/status")
async def get_agent_status(agent_name: str):
    """Get status of a specific agent"""
    if agent_name in supervisor.agents:
        return supervisor.agents[agent_name].get_status()
    return {"error": f"Agent {agent_name} not found"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "system_running": supervisor.running}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)