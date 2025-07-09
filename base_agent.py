# agents/base_agent.py
from abc import ABC, abstractmethod
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

class BaseAgent(ABC):
    """Base class for all agents in the system"""
    
    def __init__(self, name: str, logger_name: str = None):
        self.name = name
        self.logger = logging.getLogger(logger_name or name)
        self.setup_logging()
        self._status = "idle"
        self._last_activity = None
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    @abstractmethod
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the main task of the agent"""
        pass
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize the agent with configuration"""
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the agent"""
        return {
            "name": self.name,
            "status": self._status,
            "last_activity": self._last_activity
        }
    
    def update_status(self, status: str, activity: str = None):
        """Update agent status"""
        self._status = status
        self._last_activity = activity or datetime.now(timezone.utc).isoformat()