"""
Executive Reporting Agent - Generates comprehensive executive reports
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any
from datetime import datetime
import pandas as pd


class ReportingAgent:
    """Agent responsible for generating executive-level reports"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Reporting Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "ReportingAgent"
        self.logger = logger
    
    def execute(self, df: pd.DataFrame, intake_results: Dict[str, Any],
                trend_results: Dict[str, Any], root_cause_results: Dict[str, Any],
                memory_results: Dict[str, Any], strategy_results: Dict[str, Any]) -> str:
        """
        Execute report generation
        
        Args:
            df: Cleaned DataFrame
            intake_results: Results from Data Intake Agent
            trend_results: Results from Trend Detection Agent
            root_cause_results: Results from Root Cause Agent
            memory_results: Results from Memory Agent
            strategy_results: Results from Strategy Agent
            
        Returns:
            Markdown formatted executive report
        """
        with AgentTimer(self.logger, self.name):
            self.logger.log_tool_usage(self.name, "generate_executive_report")
            
            report = self._generate_report(
                df, intake_results, trend_results, root_cause_results,
                memory_results, strategy_results
            )
            
            self.logger.log_insight("Executive report generated", category="reporting")
            
            return report
    
    def _generate_report(self, df: pd.DataFrame, intake_results: Dict[str, Any],
                        trend_results: Dict[str, Any], root_cause_results: Dict[str, Any],
                        memory_results: Dict[str, Any], strategy_results: Dict[str, Any]) -> str:
        """Generate markdown report"""
        
        # Header
        report = f"""# AUTOOPS AI - Executive Business Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Analysis Period:** {intake_results['summary']['date_range']['start']} to {intake_results['summary']['date_range']['end']}  
**Data Quality:** {intake_results['summary']['quality_grade']} ({intake_results['summary']['completeness']}% complete)

---

## 📊 Executive Summary

This report analyzes {intake_results['summary']['rows']} days of business data across key performance indicators including Revenue, Customers, Conversion Rate, and Marketing Spend. The analysis identifies trends, anomalies, root causes, and provides strategic recommendations for executive decision-making.

---

## 🎯 KPI Summary

"""
        
        # KPI Summary Table
        kpi_trends = trend_results.get('kpi_trends', {})
        
        report += "| KPI | Current Avg | Trend | Growth | Volatility |\n"
        report += "|-----|------------|-------|--------|------------|\n"
        
        for kpi in ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']:
            if kpi in df.columns:
                current_avg = df[kpi].mean()
                trend_data = kpi_trends.get(kpi, {})
                
                trend_icon = "📈" if trend_data.get('trend_direction') == 'upward' else "📉" if trend_data.get('trend_direction') == 'downward' else "➡️"
                growth = trend_data.get('total_growth_pct', 0)
                volatility = trend_data.get('volatility', 0)
                
                report += f"| {kpi} | {current_avg:.2f} | {trend_icon} {trend_data.get('trend_direction', 'N/A')} | {growth:+.1f}% | {volatility:.1f}% |\n"
        
        report += "\n---\n\n"
        
        # Key Changes
        report += "## 📈 Key Changes & Trends\n\n"
        
        top_trends = trend_results.get('top_trends', [])
        for i, trend in enumerate(top_trends, 1):
            icon = "🟢" if trend['growth_pct'] > 0 else "🔴"
            report += f"{i}. **{trend['kpi']}**: {icon} {trend['direction']} trend with {trend['growth_pct']:+.1f}% growth\n"
        
        report += "\n---\n\n"
        
        # Anomaly Detection
        report += "## ⚠️ Anomaly Detection\n\n"
        
        critical_anomalies = trend_results.get('critical_anomalies', [])
        if critical_anomalies:
            report += "The following anomalies were detected in the data:\n\n"
            for i, anomaly in enumerate(critical_anomalies[:5], 1):
                report += f"{i}. **{anomaly['kpi']}** on {anomaly.get('date', 'N/A')}: Value = {anomaly['value']:.2f} (Z-score: {anomaly.get('z_score', 0):.2f})\n"
        else:
            report += "No significant anomalies detected in the analysis period.\n"
        
        report += "\n---\n\n"
        
        # Root Causes
        report += "## 🔍 Root Cause Analysis\n\n"
        
        hypotheses = root_cause_results.get('hypotheses', [])
        if hypotheses:
            report += "### Key Findings:\n\n"
            for i, hypothesis in enumerate(hypotheses[:5], 1):
                report += f"{i}. {hypothesis}\n"
        
        # Correlation insights
        report += "\n### Correlation Insights:\n\n"
        top_corr = root_cause_results.get('correlations', {}).get('top_correlations', [])
        if top_corr:
            for i, corr in enumerate(top_corr[:3], 1):
                strength = "Strong" if abs(corr['correlation']) >= 0.7 else "Moderate"
                direction = "positive" if corr['correlation'] > 0 else "negative"
                report += f"{i}. {strength} {direction} correlation between **{corr['col1']}** and **{corr['col2']}** (r={corr['correlation']:.2f})\n"
        
        report += "\n---\n\n"
        
        # Historical Comparison
        report += "## 📅 Historical Comparison\n\n"
        
        historical = memory_results.get('historical_comparison', {})
        if historical:
            report += "Comparison with 30-day historical average:\n\n"
            for kpi, comp in historical.items():
                if comp['change_pct'] is not None:
                    icon = "📈" if comp['change_pct'] > 0 else "📉"
                    report += f"- **{kpi}**: {icon} {comp['change_pct']:+.1f}% vs historical average\n"
        else:
            report += "No historical data available for comparison.\n"
        
        report += "\n---\n\n"
        
        # Strategic Recommendations
        report += "## 💡 Strategic Recommendations\n\n"
        
        recommendations = strategy_results.get('recommendations', [])
        if recommendations:
            for i, rec in enumerate(recommendations[:5], 1):
                priority_icon = "🔴" if rec['priority'] == 'critical' else "🟡" if rec['priority'] == 'high' else "🟢"
                report += f"### {i}. {priority_icon} {rec['recommendation']}\n\n"
                report += f"- **Priority:** {rec['priority'].upper()}\n"
                report += f"- **Expected Impact:** {rec['expected_impact']}\n"
                report += f"- **Timeframe:** {rec['timeframe']}\n\n"
        
        report += "---\n\n"
        
        # Action Plans
        report += "## 📋 Action Plans\n\n"
        
        action_plans = strategy_results.get('action_plans', [])
        for i, plan in enumerate(action_plans[:3], 1):
            report += f"### Plan {i}: {plan['recommendation'][:60]}...\n\n"
            report += "**Actions:**\n"
            for action in plan['actions']:
                report += f"- {action}\n"
            report += "\n**Success Metrics:**\n"
            for metric in plan['success_metrics']:
                report += f"- {metric}\n"
            report += f"\n**Timeline:** {plan['timeline']}\n\n"
        
        report += "---\n\n"
        
        # Risk Warnings
        report += "## ⚠️ Risk Warnings\n\n"
        
        risks = strategy_results.get('risks', [])
        if risks:
            for i, risk in enumerate(risks, 1):
                severity_icon = "🔴" if risk['severity'] == 'high' else "🟡"
                report += f"{i}. {severity_icon} **{risk['risk']}**\n"
                report += f"   - Impact: {risk['impact']}\n"
                report += f"   - Mitigation: {risk['mitigation']}\n\n"
        else:
            report += "No significant risks identified.\n"
        
        report += "\n---\n\n"
        
        # Opportunities
        report += "## 🎯 Opportunities\n\n"
        
        opportunities = strategy_results.get('opportunities', [])
        if opportunities:
            for i, opp in enumerate(opportunities, 1):
                report += f"{i}. **{opp['opportunity']}**\n"
                report += f"   - Recommendation: {opp['recommendation']}\n"
                report += f"   - Expected Value: {opp['expected_value']}\n\n"
        else:
            report += "No specific opportunities identified at this time.\n"
        
        report += "\n---\n\n"
        
        # Forecast
        report += "## 🔮 Forecast\n\n"
        
        forecast = strategy_results.get('forecast', {})
        if forecast:
            report += "| KPI | Current Avg | 7-Day Projection | 30-Day Projection | Confidence |\n"
            report += "|-----|-------------|------------------|-------------------|------------|\n"
            
            for kpi, proj in forecast.items():
                report += f"| {kpi} | {proj['current_avg']:.2f} | {proj['projected_7d']:.2f} | {proj['projected_30d']:.2f} | {proj['confidence']} |\n"
        
        report += "\n---\n\n"
        
        # Footer
        report += "## 📝 Notes\n\n"
        report += f"- Total sessions analyzed: {memory_results.get('memory_stats', {}).get('total_sessions', 0)}\n"
        report += f"- Historical insights stored: {memory_results.get('memory_stats', {}).get('total_insights', 0)}\n"
        report += f"- Analysis powered by AUTOOPS AI Multi-Agent System\n"
        
        return report
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Executive Reporting Agent: Synthesizes insights from all agents to generate "
            "comprehensive executive-level reports with KPI summaries, trends, root causes, "
            "recommendations, and forecasts in professional markdown format."
        )
