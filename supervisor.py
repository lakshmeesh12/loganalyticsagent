# orchestrator/supervisor.py
import asyncio
import autogen
from typing import Dict, Any, List
from datetime import datetime, timezone
from agents.log_forwarder_agent import LogForwarderAgent
from agents.monitor_agent import MonitorAgent
from agents.analyzer_agent import AnalyzerAgent
from config import AutoGenConfig
import os
from dotenv import load_dotenv
load_dotenv()


class SupervisorOrchestrator:
    """Main orchestrator that manages all agents using AutoGen"""
    
    def __init__(self):
        self.config = AutoGenConfig()
        self.agents = {
            "log_forwarder": LogForwarderAgent(),
            "monitor": MonitorAgent(), 
            "analyzer": AnalyzerAgent()
        }
        self.autogen_agents = {}
        self.group_chat = None
        self.manager = None
        self.running = False
        
    def initialize(self, config: Dict[str, Any] = None) -> bool:
        """Initialize the supervisor and all agents"""
        try:
            print("Initializing Supervisor Orchestrator...")
            
            # Initialize individual agents
            agent_config = config or {}
            for name, agent in self.agents.items():
                if not agent.initialize(agent_config):
                    print(f"Failed to initialize {name} agent")
                    return False
            
            llm_config = {
                "temperature": self.config.temperature,  # ✅ moved outside
                "config_list": [{
                    "model": self.config.model,
                    "api_key": os.getenv("OPENAI_API_KEY")
                }]
            }
            
            # Create AutoGen agents
            self._create_autogen_agents(llm_config)
            
            # Setup group chat
            self._setup_group_chat()
            
            print("Supervisor Orchestrator initialized successfully")
            return True
            
        except Exception as e:
            print(f"Failed to initialize supervisor: {e}")
            return False
    
    def _create_autogen_agents(self, llm_config: Dict[str, Any]):
        """Create AutoGen agents for communication and coordination"""
        
        # Supervisor agent
        self.autogen_agents["supervisor"] = autogen.AssistantAgent(
            name="supervisor",
            system_message="""You are the Supervisor of a log analysis system. Your responsibilities:
            1. Coordinate the execution of log forwarding, monitoring, and analysis tasks
            2. Ensure proper workflow between agents
            3. Handle error escalation and system health monitoring
            4. Provide status reports and manage the overall system state
            
            The workflow is: LogForwarder -> Monitor -> Analyzer
            Make decisions based on agent responses and coordinate next steps.""",
            llm_config=llm_config
        )
        
        # Log Forwarder representative
        self.autogen_agents["log_forwarder_rep"] = autogen.AssistantAgent(
            name="log_forwarder_rep",
            system_message="""You represent the Log Forwarder Agent. You:
            1. Report on log forwarding operations from Snowflake to CloudWatch
            2. Provide status updates on data pipeline health
            3. Alert about connection issues or data flow problems
            4. Coordinate with the supervisor for task scheduling""",
            llm_config=llm_config
        )
        
        # Monitor representative
        self.autogen_agents["monitor_rep"] = autogen.AssistantAgent(
            name="monitor_rep", 
            system_message="""You represent the Monitor Agent. You:
            1. Report on error detection in CloudWatch logs
            2. Provide alerts when errors are found
            3. Coordinate with analyzer for error processing
            4. Maintain awareness of system error patterns""",
            llm_config=llm_config
        )
        
        # Analyzer representative
        self.autogen_agents["analyzer_rep"] = autogen.AssistantAgent(
            name="analyzer_rep",
            system_message="""You represent the Analyzer Agent. You:
            1. Report on error analysis and remediation generation
            2. Provide insights on error patterns and solutions
            3. Coordinate remediation implementation
            4. Maintain knowledge base of common issues and fixes""",
            llm_config=llm_config
        )
        
        # User proxy for external interaction
        self.autogen_agents["user_proxy"] = autogen.UserProxyAgent(
            name="user_proxy",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=3,
            code_execution_config=False
        )
    
    def _setup_group_chat(self):
        """Setup AutoGen group chat for agent coordination"""
        self.group_chat = autogen.GroupChat(
            agents=list(self.autogen_agents.values()),
            messages=[],
            max_round=10
        )
        
        self.manager = autogen.GroupChatManager(
            groupchat=self.group_chat,
            llm_config={
                # "temperature": self.config.temperature,  # ✅ move it here
                "config_list": [{
                    "model": self.config.model,
                    "api_key": os.getenv("OPENAI_API_KEY")
                }]
            }
        )

    
    async def start_system(self):
        """Start the multi-agent system"""
        self.running = True
        print("Starting Multi-Agent Log Analysis System...")
        
        # Start the coordination loop
        await self._coordination_loop()
    
    async def _coordination_loop(self):
        """Main coordination loop managing agent interactions"""
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                print(f"\n--- Cycle {cycle_count} started at {datetime.now(timezone.utc)} ---")
                
                # Execute log forwarding
                forwarder_result = self.agents["log_forwarder"].execute_task({})
                
                # Execute monitoring
                monitor_result = self.agents["monitor"].execute_task({})
                
                # Execute analysis if errors were found
                analyzer_result = self.agents["analyzer"].execute_task({})
                
                # Coordinate through AutoGen
                await self._coordinate_agents(forwarder_result, monitor_result, analyzer_result)
                
                # Wait before next cycle
                await asyncio.sleep(10)
                
            except KeyboardInterrupt:
                print("System stopped by user")
                self.running = False
                break
            except Exception as e:
                print(f"Error in coordination loop: {e}")
                await asyncio.sleep(5)
    
    async def _coordinate_agents(self, forwarder_result: Dict, monitor_result: Dict, analyzer_result: Dict):
        """Coordinate agents through AutoGen group chat"""
        try:
            # Prepare status message
            status_message = f"""
            System Status Report:
            
            Log Forwarder: {forwarder_result.get('status', 'unknown')}
            - Results: {forwarder_result.get('results', {})}
            
            Monitor: {monitor_result.get('status', 'unknown')} 
            - Errors Found: {monitor_result.get('errors_found', 0)}
            - Log Groups Checked: {monitor_result.get('log_groups_checked', 0)}
            
            Analyzer: {analyzer_result.get('status', 'unknown')}
            - Remediations Generated: {analyzer_result.get('remediations_generated', 0)}
            
            Please coordinate next steps and provide system health assessment.
            """
            
            # Initiate group chat coordination
            self.manager.initiate_chat(
                message=status_message,
                sender=self.autogen_agents["user_proxy"],
                recipient=self.autogen_agents["supervisor"]  # ✅ Added recipient
            )


            
        except Exception as e:
            print(f"Error in agent coordination: {e}")
    
    def stop_system(self):
        """Stop the multi-agent system"""
        self.running = False
        print("Stopping Multi-Agent Log Analysis System...")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        return {
            "running": self.running,
            "agents": {name: agent.get_status() for name, agent in self.agents.items()},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def add_agent(self, name: str, agent_class, config: Dict[str, Any] = None):
        """Add a new agent to the system (for extensibility)"""
        try:
            new_agent = agent_class()
            if new_agent.initialize(config or {}):
                self.agents[name] = new_agent
                print(f"Successfully added agent: {name}")
                return True
            else:
                print(f"Failed to initialize new agent: {name}")
                return False
        except Exception as e:
            print(f"Error adding agent {name}: {e}")
            return False
    
    def remove_agent(self, name: str):
        """Remove an agent from the system"""
        if name in self.agents:
            del self.agents[name]
            print(f"Removed agent: {name}")
            return True
        else:
            print(f"Agent {name} not found")
            return False
