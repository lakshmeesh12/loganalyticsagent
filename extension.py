from abc import ABC, abstractmethod
from typing import Dict, Any, List
from agents.base_agent import BaseAgent

class ExtensibleAgent(BaseAgent):
    """Base class for easily extensible agents"""
    
    def __init__(self, name: str, plugins: List = None):
        super().__init__(name)
        self.plugins = plugins or []
        self.hooks = {
            "before_execute": [],
            "after_execute": [],
            "on_error": []
        }
    
    def add_plugin(self, plugin):
        """Add a plugin to the agent"""
        self.plugins.append(plugin)
    
    def add_hook(self, hook_name: str, callback):
        """Add a hook callback"""
        if hook_name in self.hooks:
            self.hooks[hook_name].append(callback)
    
    def execute_hooks(self, hook_name: str, *args, **kwargs):
        """Execute all callbacks for a hook"""
        for callback in self.hooks.get(hook_name, []):
            try:
                callback(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Hook {hook_name} failed: {e}")

class Plugin(ABC):
    """Base class for agent plugins"""
    
    @abstractmethod
    def initialize(self, agent: BaseAgent) -> bool:
        """Initialize the plugin with the agent"""
        pass
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute plugin functionality"""
        pass

# Example extension: Alert Plugin
class AlertPlugin(Plugin):
    """Plugin for sending alerts when errors are detected"""
    
    def __init__(self, alert_config: Dict[str, Any]):
        self.config = alert_config
        self.alert_channels = []
    
    def initialize(self, agent: BaseAgent) -> bool:
        """Initialize alert plugin"""
        # Setup alert channels (email, slack, etc.)
        return True
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Send alerts based on context"""
        if context.get("error_detected"):
            # Send alert through configured channels
            return {"alert_sent": True}
        return {"alert_sent": False}