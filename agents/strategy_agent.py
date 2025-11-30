"""
Strategy Agent - Generates business recommendations and action plans
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any, List
import pandas as pd


class StrategyAgent:
    """Agent responsible for strategic recommendations and forecasting"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Strategy Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "StrategyAgent"
        self.logger = logger
    
    def execute(self, df: pd.DataFrame, trend_results: Dict[str, Any], 
                root_cause_results: Dict[str, Any], 
                memory_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute strategy generation
        
        Args:
            df: Cleaned DataFrame
            trend_results: Results from Trend Detection Agent
            root_cause_results: Results from Root Cause Agent
            memory_results: Results from Memory Agent
            
        Returns:
            Strategic recommendations and forecasts
        """
        with AgentTimer(self.logger, self.name):
            results = {
                'recommendations': [],
                'action_plans': [],
                'risks': [],
                'opportunities': [],
                'forecast': {}
            }
            
            # Generate recommendations based on trends
            results['recommendations'] = self._generate_recommendations(
                trend_results, root_cause_results, memory_results
            )
            
            # Create action plans
            results['action_plans'] = self._create_action_plans(
                results['recommendations'], trend_results
            )
            
            # Identify risks
            results['risks'] = self._identify_risks(trend_results, root_cause_results)
            
            # Identify opportunities
            results['opportunities'] = self._identify_opportunities(
                trend_results, root_cause_results
            )
            
            # Generate forecast
            results['forecast'] = self._generate_forecast(df, trend_results)
            
            # Log key recommendations
            for i, rec in enumerate(results['recommendations'][:3], 1):
                self.logger.log_insight(f"Recommendation {i}: {rec['recommendation']}", 
                                       category="strategy")
            
            return results
    
    def _generate_recommendations(self, trend_results: Dict[str, Any], 
                                 root_cause_results: Dict[str, Any],
                                 memory_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate strategic recommendations"""
        recommendations = []
        
        # Analyze top trends
        top_trends = trend_results.get('top_trends', [])
        
        for trend in top_trends:
            kpi = trend['kpi']
            growth = trend['growth_pct']
            
            if kpi == 'Revenue':
                if growth > 10:
                    recommendations.append({
                        'priority': 'high',
                        'category': 'growth',
                        'kpi': kpi,
                        'recommendation': f"Capitalize on {growth:.1f}% revenue growth by scaling successful channels and increasing marketing investment",
                        'expected_impact': 'high',
                        'timeframe': 'immediate'
                    })
                elif growth < -10:
                    recommendations.append({
                        'priority': 'critical',
                        'category': 'recovery',
                        'kpi': kpi,
                        'recommendation': f"Address {abs(growth):.1f}% revenue decline through customer retention programs and product optimization",
                        'expected_impact': 'high',
                        'timeframe': 'immediate'
                    })
            
            elif kpi == 'Customers':
                if growth > 15:
                    recommendations.append({
                        'priority': 'high',
                        'category': 'retention',
                        'kpi': kpi,
                        'recommendation': f"Focus on customer retention to maintain {growth:.1f}% customer growth momentum",
                        'expected_impact': 'medium',
                        'timeframe': 'short-term'
                    })
                elif growth < -10:
                    recommendations.append({
                        'priority': 'critical',
                        'category': 'acquisition',
                        'kpi': kpi,
                        'recommendation': f"Implement aggressive customer acquisition strategy to reverse {abs(growth):.1f}% customer decline",
                        'expected_impact': 'high',
                        'timeframe': 'immediate'
                    })
            
            elif kpi == 'Conversion_Rate':
                if growth < -5:
                    recommendations.append({
                        'priority': 'high',
                        'category': 'optimization',
                        'kpi': kpi,
                        'recommendation': f"Optimize conversion funnel to address {abs(growth):.1f}% conversion rate decline",
                        'expected_impact': 'medium',
                        'timeframe': 'short-term'
                    })
            
            elif kpi == 'Marketing_Spend':
                if growth > 20:
                    recommendations.append({
                        'priority': 'medium',
                        'category': 'efficiency',
                        'kpi': kpi,
                        'recommendation': f"Evaluate marketing ROI given {growth:.1f}% spend increase - ensure efficiency",
                        'expected_impact': 'medium',
                        'timeframe': 'short-term'
                    })
        
        # Add recommendations based on correlations
        drivers = root_cause_results.get('drivers', [])
        for driver_info in drivers:
            if driver_info['potential_drivers']:
                for driver in driver_info['potential_drivers'][:1]:  # Top driver
                    if driver['strength'] == 'strong':
                        recommendations.append({
                            'priority': 'medium',
                            'category': 'optimization',
                            'kpi': driver_info['kpi'],
                            'recommendation': f"Leverage strong correlation between {driver_info['kpi']} and {driver['driver']} for strategic planning",
                            'expected_impact': 'medium',
                            'timeframe': 'medium-term'
                        })
        
        # Sort by priority
        priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 3))
        
        return recommendations
    
    def _create_action_plans(self, recommendations: List[Dict[str, Any]], 
                            trend_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create actionable plans"""
        action_plans = []
        
        for rec in recommendations[:5]:  # Top 5 recommendations
            plan = {
                'recommendation': rec['recommendation'],
                'priority': rec['priority'],
                'actions': [],
                'success_metrics': [],
                'timeline': rec['timeframe']
            }
            
            # Generate specific actions based on category
            if rec['category'] == 'growth':
                plan['actions'] = [
                    "Analyze top-performing channels and allocate additional budget",
                    "Expand successful campaigns to new customer segments",
                    "Implement referral program to accelerate growth"
                ]
                plan['success_metrics'] = [
                    "20% increase in customer acquisition",
                    "15% improvement in customer lifetime value"
                ]
            
            elif rec['category'] == 'recovery':
                plan['actions'] = [
                    "Conduct customer feedback survey to identify pain points",
                    "Launch win-back campaign for churned customers",
                    "Review and optimize pricing strategy"
                ]
                plan['success_metrics'] = [
                    "Reduce churn rate by 25%",
                    "Increase customer satisfaction score by 15%"
                ]
            
            elif rec['category'] == 'optimization':
                plan['actions'] = [
                    "A/B test key conversion points in customer journey",
                    "Implement personalization in marketing campaigns",
                    "Optimize landing pages for better conversion"
                ]
                plan['success_metrics'] = [
                    "10% improvement in conversion rate",
                    "Reduce cost per acquisition by 15%"
                ]
            
            elif rec['category'] == 'efficiency':
                plan['actions'] = [
                    "Analyze marketing spend ROI by channel",
                    "Reallocate budget from low-performing to high-performing channels",
                    "Implement marketing automation to reduce costs"
                ]
                plan['success_metrics'] = [
                    "Improve marketing ROI by 20%",
                    "Reduce customer acquisition cost by 15%"
                ]
            
            action_plans.append(plan)
        
        return action_plans
    
    def _identify_risks(self, trend_results: Dict[str, Any], 
                       root_cause_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify business risks"""
        risks = []
        
        # Check for declining trends
        top_trends = trend_results.get('top_trends', [])
        for trend in top_trends:
            if trend['growth_pct'] < -10:
                risks.append({
                    'severity': 'high' if trend['growth_pct'] < -20 else 'medium',
                    'risk': f"{trend['kpi']} declining by {abs(trend['growth_pct']):.1f}%",
                    'impact': 'Revenue and business growth at risk',
                    'mitigation': f"Implement recovery strategy for {trend['kpi']}"
                })
        
        # Check for high volatility
        for trend in top_trends:
            if trend.get('volatility', 0) > 30:
                risks.append({
                    'severity': 'medium',
                    'risk': f"High volatility in {trend['kpi']} ({trend['volatility']:.1f}%)",
                    'impact': 'Unpredictable performance makes planning difficult',
                    'mitigation': 'Identify and stabilize volatile factors'
                })
        
        return risks
    
    def _identify_opportunities(self, trend_results: Dict[str, Any], 
                               root_cause_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify business opportunities"""
        opportunities = []
        
        # Check for positive trends
        top_trends = trend_results.get('top_trends', [])
        for trend in top_trends:
            if trend['growth_pct'] > 15:
                opportunities.append({
                    'potential': 'high',
                    'opportunity': f"Strong {trend['kpi']} growth ({trend['growth_pct']:.1f}%)",
                    'recommendation': f"Scale initiatives driving {trend['kpi']} growth",
                    'expected_value': 'Accelerated business growth'
                })
        
        # Check channel performance
        channel_analysis = root_cause_results.get('channel_analysis', {})
        if channel_analysis:
            best_channel = max(channel_analysis.items(), 
                             key=lambda x: x[1].get('avg_revenue', 0))
            opportunities.append({
                'potential': 'medium',
                'opportunity': f"Best performing channel: {best_channel[0]}",
                'recommendation': f"Increase investment in {best_channel[0]} channel",
                'expected_value': 'Higher ROI on marketing spend'
            })
        
        return opportunities
    
    def _generate_forecast(self, df: pd.DataFrame, 
                          trend_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate simple forecast"""
        forecast = {}
        
        kpi_trends = trend_results.get('kpi_trends', {})
        
        for kpi, trend_data in kpi_trends.items():
            if kpi in df.columns and trend_data:
                current_avg = df[kpi].tail(7).mean()  # Last week average
                growth_rate = trend_data.get('avg_period_change_pct', 0) / 100
                
                # Simple linear projection
                forecast[kpi] = {
                    'current_avg': round(current_avg, 2),
                    'projected_7d': round(current_avg * (1 + growth_rate * 7), 2),
                    'projected_30d': round(current_avg * (1 + growth_rate * 30), 2),
                    'confidence': 'medium' if abs(growth_rate) < 0.05 else 'low'
                }
        
        return forecast
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Strategy Agent: Generates business recommendations, creates action plans, "
            "identifies risks and opportunities, and produces forecasts based on "
            "trend and root cause analysis."
        )
