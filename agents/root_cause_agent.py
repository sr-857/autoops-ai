"""
Root Cause Analysis Agent - Investigates correlations and drivers of change
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.stats_tools import StatsTools
from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any, List
import pandas as pd


class RootCauseAgent:
    """Agent responsible for root cause analysis and correlation investigation"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Root Cause Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "RootCauseAgent"
        self.logger = logger
        self.stats_tools = StatsTools()
        self.kpi_columns = ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']
    
    def execute(self, df: pd.DataFrame, trend_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute root cause analysis
        
        Args:
            df: Cleaned DataFrame
            trend_results: Results from Trend Detection Agent
            
        Returns:
            Root cause analysis results
        """
        with AgentTimer(self.logger, self.name):
            results = {
                'correlations': {},
                'drivers': [],
                'hypotheses': []
            }
            
            # Calculate correlation matrix
            self.logger.log_tool_usage(self.name, "stats_tools.correlation_matrix")
            corr_matrix = self.stats_tools.correlation_matrix(df, self.kpi_columns)
            results['correlations'] = corr_matrix
            
            # Log top correlations
            if 'top_correlations' in corr_matrix:
                for corr in corr_matrix['top_correlations'][:3]:
                    insight = (f"Correlation: {corr['col1']} ↔ {corr['col2']} "
                             f"(r={corr['correlation']})")
                    self.logger.log_insight(insight, category="correlation")
            
            # Analyze drivers for top trends
            top_trends = trend_results.get('top_trends', [])
            for trend in top_trends:
                drivers = self._analyze_drivers(df, trend['kpi'], corr_matrix)
                results['drivers'].append({
                    'kpi': trend['kpi'],
                    'growth_pct': trend['growth_pct'],
                    'potential_drivers': drivers
                })
            
            # Generate hypotheses
            results['hypotheses'] = self._generate_hypotheses(df, results['drivers'], 
                                                              trend_results)
            
            # Analyze channel performance
            if 'Channel' in df.columns:
                results['channel_analysis'] = self._analyze_channels(df)
            
            return results
    
    def _analyze_drivers(self, df: pd.DataFrame, target_kpi: str, 
                        corr_matrix: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify potential drivers for a KPI"""
        drivers = []
        
        if 'top_correlations' not in corr_matrix:
            return drivers
        
        # Find correlations involving target KPI
        for corr in corr_matrix['top_correlations']:
            if corr['col1'] == target_kpi or corr['col2'] == target_kpi:
                other_col = corr['col2'] if corr['col1'] == target_kpi else corr['col1']
                
                # Determine if it's a driver (strong correlation)
                if abs(corr['correlation']) >= 0.5:
                    drivers.append({
                        'driver': other_col,
                        'correlation': corr['correlation'],
                        'strength': 'strong' if abs(corr['correlation']) >= 0.7 else 'moderate'
                    })
        
        return drivers
    
    def _generate_hypotheses(self, df: pd.DataFrame, drivers: List[Dict[str, Any]], 
                            trend_results: Dict[str, Any]) -> List[str]:
        """Generate root cause hypotheses"""
        hypotheses = []
        
        for driver_info in drivers:
            kpi = driver_info['kpi']
            potential_drivers = driver_info['potential_drivers']
            growth = driver_info['growth_pct']
            
            if growth > 10:
                direction = "increase"
            elif growth < -10:
                direction = "decrease"
            else:
                continue
            
            # Generate hypotheses based on drivers
            for driver in potential_drivers:
                if driver['correlation'] > 0.5:
                    hypothesis = (
                        f"The {direction} in {kpi} may be driven by changes in "
                        f"{driver['driver']} (correlation: {driver['correlation']:.2f})"
                    )
                    hypotheses.append(hypothesis)
                    self.logger.log_insight(hypothesis, category="hypothesis")
                elif driver['correlation'] < -0.5:
                    hypothesis = (
                        f"The {direction} in {kpi} shows inverse relationship with "
                        f"{driver['driver']} (correlation: {driver['correlation']:.2f})"
                    )
                    hypotheses.append(hypothesis)
                    self.logger.log_insight(hypothesis, category="hypothesis")
        
        # Add general hypotheses if no specific drivers found
        if not hypotheses:
            hypotheses.append(
                "No strong correlations detected. Changes may be due to external factors "
                "not captured in the current dataset."
            )
        
        return hypotheses
    
    def _analyze_channels(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze performance by channel"""
        channel_stats = {}
        
        for channel in df['Channel'].unique():
            channel_df = df[df['Channel'] == channel]
            
            channel_stats[channel] = {
                'records': len(channel_df),
                'avg_revenue': float(channel_df['Revenue'].mean()) if 'Revenue' in df.columns else 0,
                'avg_customers': float(channel_df['Customers'].mean()) if 'Customers' in df.columns else 0,
                'avg_conversion': float(channel_df['Conversion_Rate'].mean()) if 'Conversion_Rate' in df.columns else 0
            }
        
        # Find best performing channel
        if channel_stats:
            best_channel = max(channel_stats.items(), 
                             key=lambda x: x[1]['avg_revenue'])
            
            insight = f"Best performing channel: {best_channel[0]} (avg revenue: {best_channel[1]['avg_revenue']:.2f})"
            self.logger.log_insight(insight, category="channel_performance")
        
        return channel_stats
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Root Cause Analysis Agent: Investigates correlations between metrics, "
            "identifies drivers of KPI changes, generates hypotheses about causal "
            "relationships, and analyzes channel performance."
        )
