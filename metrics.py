from typing import Dict, Any, List
from datetime import datetime, timezone
import json

class MetricsCollector:
    """Collect and manage system metrics"""
    
    def __init__(self):
        self.metrics = {
            "logs_processed": 0,
            "errors_detected": 0,
            "remediations_generated": 0,
            "system_uptime": None,
            "agent_performance": {}
        }
        self.start_time = datetime.now(timezone.utc)
    
    def update_metric(self, metric_name: str, value: Any):
        """Update a specific metric"""
        self.metrics[metric_name] = value
    
    def increment_counter(self, counter_name: str, increment: int = 1):
        """Increment a counter metric"""
        if counter_name in self.metrics:
            self.metrics[counter_name] += increment
        else:
            self.metrics[counter_name] = increment
    
    def record_agent_performance(self, agent_name: str, execution_time: float, status: str):
        """Record agent performance metrics"""
        if agent_name not in self.metrics["agent_performance"]:
            self.metrics["agent_performance"][agent_name] = {
                "total_executions": 0,
                "average_execution_time": 0,
                "success_rate": 0,
                "last_execution": None
            }
        
        perf = self.metrics["agent_performance"][agent_name]
        perf["total_executions"] += 1
        perf["last_execution"] = datetime.now(timezone.utc).isoformat()
        
        # Update average execution time
        current_avg = perf["average_execution_time"]
        perf["average_execution_time"] = (
            (current_avg * (perf["total_executions"] - 1) + execution_time) / 
            perf["total_executions"]
        )
        
        # Update success rate (simplified)
        if status == "success":
            success_count = perf.get("success_count", 0) + 1
            perf["success_count"] = success_count
            perf["success_rate"] = success_count / perf["total_executions"]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        self.metrics["system_uptime"] = (
            datetime.now(timezone.utc) - self.start_time
        ).total_seconds()
        
        return self.metrics.copy()
    
    def export_metrics(self, filename: str):
        """Export metrics to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.get_metrics(), f, indent=2, default=str)