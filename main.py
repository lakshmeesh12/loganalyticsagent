from fastapi import FastAPI
import subprocess
import threading
import sys
import os

app = FastAPI()
VENV_PYTHON = sys.executable  # Uses current interpreter path


def run_script(path):
    process = subprocess.Popen(
        [VENV_PYTHON, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    for line in iter(process.stdout.readline, ''):
        print(line.strip())

@app.get("/start-logging")
def start_logging():
    # Start log forwarder
    threading.Thread(
        target=run_script,
        args=("log_forwarder.py",),
        daemon=True
    ).start()
    # Start monitor
    threading.Thread(
        target=run_script,
        args=("monitor.py",),
        daemon=True
    ).start()
    # Start AI agent for error analysis
    threading.Thread(
        target=run_script,
        args=("agent.py",),
        daemon=True
    ).start()
    return {"status": "Log forwarder, monitor, and AI agent are running in parallel!"}
