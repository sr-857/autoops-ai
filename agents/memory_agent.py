"""
Memory Agent - Manages long-term memory and historical comparisons
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.memory_store import MemoryStore
from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any
import pandas as pd


class MemoryAgent:
    """Agent responsible for long-term memory management"""
    
    def __init__(self, logger: ObservabilityLogger, memory_file: str):
        """
        Initialize Memory Agent
        
        Args:
            logger: ObservabilityLogger instance
            memory_file: Path to memory JSON file
        """
        self.name = "MemoryAgent"
        self.logger = logger
        self.memory_store = MemoryStore(memory_file)
    
    def execute(self, df: pd.DataFrame, trend_results: Dict[str, Any], 
                root_cause_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute memory storage and historical comparison
        
        Args:
            df: Cleaned DataFrame
            trend_results: Results from Trend Detection Agent
            root_cause_results: Results from Root Cause Agent
            
        Returns:
            Memory analysis results
        """
        with AgentTimer(self.logger, self.name):
            results = {
                'session_stored': False,
                'kpis_stored': False,
                'historical_comparison': {},
                'insights_stored': []
            }
            
            # Calculate current KPI averages
            current_kpis = self._calculate_current_kpis(df)
            
            # Compare with historical data
            self.logger.log_tool_usage(self.name, "memory_store.compare_with_history")
            comparison = self.memory_store.compare_with_history(current_kpis, lookback_days=30)
            results['historical_comparison'] = comparison
            
            # Log comparison insights
            for kpi, comp_data in comparison.items():
                if comp_data['change_pct'] is not None:
                    insight = (f"{kpi}: {comp_data['change_pct']:.1f}% vs 30-day average "
                             f"(current: {comp_data['current']:.2f}, "
                             f"historical: {comp_data['historical_avg']:.2f})")
                    self.logger.log_insight(insight, category="historical_comparison")
            
            # Store current session
            session_data = {
                'date_range': {
                    'start': df['Date'].min().strftime('%Y-%m-%d'),
                    'end': df['Date'].max().strftime('%Y-%m-%d')
                },
                'kpis': current_kpis,
                'top_trends': trend_results.get('top_trends', []),
                'key_hypotheses': root_cause_results.get('hypotheses', [])[:3]
            }
            
            self.logger.log_tool_usage(self.name, "memory_store.store_session")
            session_id = self.memory_store.store_session(session_data)
            results['session_stored'] = True
            results['session_id'] = session_id
            
            self.logger.log_insight(f"Session stored: {session_id}", category="memory")
                
            # Store KPI snapshots for all dates (batched for performance)
            self.logger.log_tool_usage(self.name, "memory_store.store_kpi_snapshots_batch")
            kpi_snapshots = {}
            for _, row in df.iterrows():
                date_str = row['Date'].strftime('%Y-%m-%d')
                kpi_snapshots[date_str] = {
                    'Revenue': float(row['Revenue']) if 'Revenue' in df.columns else 0,
                    'Customers': float(row['Customers']) if 'Customers' in df.columns else 0,
                    'Conversion_Rate': float(row['Conversion_Rate']) if 'Conversion_Rate' in df.columns else 0,
                    'Marketing_Spend': float(row['Marketing_Spend']) if 'Marketing_Spend' in df.columns else 0
                }
            
            # Store all snapshots at once
            self.memory_store.store_kpi_snapshots_batch(kpi_snapshots)
            results['kpis_stored'] = True
            
            # Store key insights
            for hypothesis in root_cause_results.get('hypotheses', [])[:3]:
                insight_data = {
                    'category': 'hypothesis',
                    'text': hypothesis,
                    'source': 'RootCauseAgent'
                }
                self.memory_store.store_insight(insight_data)
                results['insights_stored'].append(hypothesis)
            
            # Get memory statistics
            results['memory_stats'] = self.memory_store.get_memory_stats()
            
            return results
    
    def _calculate_current_kpis(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate current KPI averages"""
        kpis = {}
        
        kpi_columns = ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']
        
        for col in kpi_columns:
            if col in df.columns:
                kpis[col] = float(df[col].mean())
        
        return kpis
    
    def get_recent_sessions(self, n: int = 3) -> Dict[str, Any]:
        """Retrieve recent sessions"""
        sessions = self.memory_store.get_recent_sessions(n)
        return {'sessions': sessions}
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Memory Agent: Manages long-term memory by storing session data, "
            "KPI snapshots, and insights. Enables historical comparisons and "
            "tracks performance over time."
        )
