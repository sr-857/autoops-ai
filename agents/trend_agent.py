"""
Trend Detection Agent - Analyzes KPI trends and detects anomalies
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.stats_tools import StatsTools
from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any, List
import pandas as pd


class TrendDetectionAgent:
    """Agent responsible for trend analysis and anomaly detection"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Trend Detection Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "TrendDetectionAgent"
        self.logger = logger
        self.stats_tools = StatsTools()
        self.kpi_columns = ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']
    
    def execute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Execute trend detection and anomaly analysis
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Trend analysis results
        """
        with AgentTimer(self.logger, self.name):
            results = {
                'kpi_trends': {},
                'anomalies': {},
                'key_findings': []
            }
            
            # Analyze each KPI
            for kpi in self.kpi_columns:
                if kpi not in df.columns:
                    continue
                
                self.logger.log_tool_usage(self.name, "stats_tools.detect_trend", 
                                          {"column": kpi})
                
                # Detect trends
                trend = self.stats_tools.detect_trend(df, kpi)
                results['kpi_trends'][kpi] = trend
                
                # Detect anomalies
                anomalies = self.stats_tools.detect_anomalies(df, kpi, method='zscore')
                results['anomalies'][kpi] = anomalies
                
                # Log insights
                if trend:
                    insight = (f"{kpi}: {trend['trend_direction']} trend, "
                             f"{trend['total_growth_pct']}% total growth")
                    self.logger.log_insight(insight, category="trend")
                    results['key_findings'].append(insight)
                
                if anomalies and anomalies['anomalies_found'] > 0:
                    insight = f"{kpi}: {anomalies['anomalies_found']} anomalies detected"
                    self.logger.log_insight(insight, category="anomaly")
                    results['key_findings'].append(insight)
                
                # Log metrics
                if trend:
                    self.logger.log_metric(f"{kpi}_growth_rate", trend['total_growth_pct'])
                    self.logger.log_metric(f"{kpi}_volatility", trend['volatility'])
            
            # Identify most significant trends
            results['top_trends'] = self._identify_top_trends(results['kpi_trends'])
            results['critical_anomalies'] = self._identify_critical_anomalies(results['anomalies'])
            
            return results
    
    def _identify_top_trends(self, kpi_trends: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify most significant trends"""
        trends = []
        
        for kpi, trend_data in kpi_trends.items():
            if trend_data and 'total_growth_pct' in trend_data:
                trends.append({
                    'kpi': kpi,
                    'growth_pct': trend_data['total_growth_pct'],
                    'direction': trend_data['trend_direction'],
                    'volatility': trend_data.get('volatility', 0)
                })
        
        # Sort by absolute growth
        trends.sort(key=lambda x: abs(x['growth_pct']), reverse=True)
        
        return trends[:3]  # Top 3
    
    def _identify_critical_anomalies(self, anomalies: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify critical anomalies"""
        critical = []
        
        for kpi, anomaly_data in anomalies.items():
            if anomaly_data and anomaly_data['anomalies_found'] > 0:
                # Get most extreme anomalies
                for anomaly in anomaly_data['anomalies'][:3]:  # Top 3 per KPI
                    critical.append({
                        'kpi': kpi,
                        'date': anomaly.get('date'),
                        'value': anomaly['value'],
                        'z_score': anomaly.get('z_score', 0)
                    })
        
        # Sort by z-score
        critical.sort(key=lambda x: abs(x.get('z_score', 0)), reverse=True)
        
        return critical[:5]  # Top 5 overall
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Trend Detection Agent: Analyzes KPI trends over time, "
            "detects growth/decline patterns, identifies anomalies using "
            "statistical methods, and highlights key changes."
        )
