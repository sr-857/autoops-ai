"""
Logging Tools - Observability, tracing, and metrics for multi-agent system
Part of AUTOOPS AI Multi-Agent System
"""

import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
import time


class ObservabilityLogger:
    """Structured logging and tracing for agent activities"""
    
    def __init__(self, log_file: str, log_level: int = logging.INFO):
        """
        Initialize observability logger
        
        Args:
            log_file: Path to log file
            log_level: Logging level
        """
        self.log_file = log_file
        self.session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.traces = []
        self.metrics = {}
        
        # Create logs directory if needed
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure logging
        self.logger = logging.getLogger('AUTOOPS_AI')
        self.logger.setLevel(log_level)
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"=== AUTOOPS AI Session Started: {self.session_id} ===")
    
    def log_agent_start(self, agent_name: str, input_data: Optional[Dict[str, Any]] = None):
        """
        Log agent execution start
        
        Args:
            agent_name: Name of the agent
            input_data: Optional input data summary
        """
        trace = {
            'agent': agent_name,
            'action': 'start',
            'timestamp': datetime.now().isoformat(),
            'input_summary': input_data or {}
        }
        self.traces.append(trace)
        
        self.logger.info(f"🤖 Agent [{agent_name}] started")
        if input_data:
            self.logger.debug(f"   Input: {json.dumps(input_data, indent=2)}")
    
    def log_agent_end(self, agent_name: str, output_data: Optional[Dict[str, Any]] = None, 
                      duration: Optional[float] = None):
        """
        Log agent execution completion
        
        Args:
            agent_name: Name of the agent
            output_data: Optional output data summary
            duration: Execution duration in seconds
        """
        trace = {
            'agent': agent_name,
            'action': 'end',
            'timestamp': datetime.now().isoformat(),
            'output_summary': output_data or {},
            'duration_seconds': duration
        }
        self.traces.append(trace)
        
        duration_str = f" ({duration:.2f}s)" if duration else ""
        self.logger.info(f"✅ Agent [{agent_name}] completed{duration_str}")
        if output_data:
            self.logger.debug(f"   Output: {json.dumps(output_data, indent=2)}")
    
    def log_agent_error(self, agent_name: str, error: Exception):
        """
        Log agent error
        
        Args:
            agent_name: Name of the agent
            error: Exception that occurred
        """
        trace = {
            'agent': agent_name,
            'action': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(error),
            'error_type': type(error).__name__
        }
        self.traces.append(trace)
        
        self.logger.error(f"❌ Agent [{agent_name}] error: {error}")
    
    def log_tool_usage(self, agent_name: str, tool_name: str, params: Optional[Dict[str, Any]] = None):
        """
        Log tool usage by an agent
        
        Args:
            agent_name: Name of the agent
            tool_name: Name of the tool
            params: Optional tool parameters
        """
        self.logger.debug(f"🔧 [{agent_name}] using tool: {tool_name}")
        if params:
            self.logger.debug(f"   Params: {json.dumps(params, indent=2)}")
    
    def log_metric(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Log a metric
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            tags: Optional tags
        """
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append({
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'tags': tags or {}
        })
        
        self.logger.debug(f"📊 Metric [{metric_name}]: {value}")
    
    def log_insight(self, insight: str, category: str = 'general'):
        """
        Log a business insight
        
        Args:
            insight: Insight text
            category: Insight category
        """
        self.logger.info(f"💡 Insight [{category}]: {insight}")
    
    def log_decision(self, decision: str, reasoning: str):
        """
        Log an agent decision
        
        Args:
            decision: Decision made
            reasoning: Reasoning behind decision
        """
        self.logger.info(f"🎯 Decision: {decision}")
        self.logger.debug(f"   Reasoning: {reasoning}")
    
    def get_session_trace(self) -> Dict[str, Any]:
        """
        Get complete session trace
        
        Returns:
            Session trace dictionary
        """
        return {
            'session_id': self.session_id,
            'traces': self.traces,
            'metrics': self.metrics,
            'generated_at': datetime.now().isoformat()
        }
    
    def save_trace(self, output_file: str):
        """
        Save session trace to JSON file
        
        Args:
            output_file: Path to output file
        """
        trace = self.get_session_trace()
        
        with open(output_file, 'w') as f:
            json.dump(trace, f, indent=2)
        
        self.logger.info(f"📝 Session trace saved to: {output_file}")
    
    def get_agent_metrics(self) -> Dict[str, Any]:
        """
        Calculate agent performance metrics
        
        Returns:
            Metrics dictionary
        """
        agent_stats = {}
        
        for trace in self.traces:
            agent = trace['agent']
            
            if agent not in agent_stats:
                agent_stats[agent] = {
                    'executions': 0,
                    'errors': 0,
                    'total_duration': 0,
                    'avg_duration': 0
                }
            
            if trace['action'] == 'start':
                agent_stats[agent]['executions'] += 1
            elif trace['action'] == 'error':
                agent_stats[agent]['errors'] += 1
            elif trace['action'] == 'end' and trace.get('duration_seconds'):
                agent_stats[agent]['total_duration'] += trace['duration_seconds']
        
        # Calculate averages
        for agent, stats in agent_stats.items():
            if stats['executions'] > 0:
                stats['avg_duration'] = round(stats['total_duration'] / stats['executions'], 2)
        
        return agent_stats
    
    def log_summary(self):
        """Log session summary"""
        agent_metrics = self.get_agent_metrics()
        
        self.logger.info("=" * 60)
        self.logger.info("SESSION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Session ID: {self.session_id}")
        self.logger.info(f"Total Traces: {len(self.traces)}")
        self.logger.info(f"Agents Executed: {len(agent_metrics)}")
        
        for agent, stats in agent_metrics.items():
            self.logger.info(f"  - {agent}: {stats['executions']} executions, "
                           f"{stats['errors']} errors, "
                           f"{stats['avg_duration']}s avg duration")
        
        self.logger.info("=" * 60)


class AgentTimer:
    """Context manager for timing agent execution"""
    
    def __init__(self, logger: ObservabilityLogger, agent_name: str):
        """
        Initialize timer
        
        Args:
            logger: ObservabilityLogger instance
            agent_name: Name of the agent
        """
        self.logger = logger
        self.agent_name = agent_name
        self.start_time = None
        self.duration = None
    
    def __enter__(self):
        """Start timer"""
        self.start_time = time.time()
        self.logger.log_agent_start(self.agent_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timer"""
        self.duration = time.time() - self.start_time
        
        if exc_type is not None:
            self.logger.log_agent_error(self.agent_name, exc_val)
        else:
            self.logger.log_agent_end(self.agent_name, duration=self.duration)
        
        return False  # Don't suppress exceptions
