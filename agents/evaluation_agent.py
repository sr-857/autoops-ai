"""
Evaluation & QA Agent - Scores and evaluates system outputs
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any, List


class EvaluationAgent:
    """Agent responsible for evaluating report quality and system performance"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Evaluation Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "EvaluationAgent"
        self.logger = logger
    
    def execute(self, report: str, trend_results: Dict[str, Any],
                root_cause_results: Dict[str, Any], 
                strategy_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute evaluation and scoring
        
        Args:
            report: Generated executive report
            trend_results: Results from Trend Detection Agent
            root_cause_results: Results from Root Cause Agent
            strategy_results: Results from Strategy Agent
            
        Returns:
            Evaluation results with scores and feedback
        """
        with AgentTimer(self.logger, self.name):
            results = {
                'clarity_score': 0,
                'logic_score': 0,
                'actionability_score': 0,
                'overall_score': 0,
                'confidence_score': 0,
                'strengths': [],
                'weaknesses': [],
                'improvement_suggestions': []
            }
            
            # Evaluate clarity
            results['clarity_score'] = self._evaluate_clarity(report)
            
            # Evaluate logical consistency
            results['logic_score'] = self._evaluate_logic(
                trend_results, root_cause_results, strategy_results
            )
            
            # Evaluate actionability
            results['actionability_score'] = self._evaluate_actionability(
                strategy_results
            )
            
            # Calculate overall score
            results['overall_score'] = round(
                (results['clarity_score'] + results['logic_score'] + 
                 results['actionability_score']) / 3, 1
            )
            
            # Calculate confidence score
            results['confidence_score'] = self._calculate_confidence(
                trend_results, root_cause_results, strategy_results
            )
            
            # Identify strengths and weaknesses
            results['strengths'] = self._identify_strengths(results)
            results['weaknesses'] = self._identify_weaknesses(results)
            
            # Generate improvement suggestions
            results['improvement_suggestions'] = self._generate_suggestions(results)
            
            # Log evaluation results
            self.logger.log_insight(
                f"Overall Quality Score: {results['overall_score']}/10",
                category="evaluation"
            )
            self.logger.log_insight(
                f"Confidence Score: {results['confidence_score']}/10",
                category="evaluation"
            )
            
            self.logger.log_metric("report_quality_score", results['overall_score'])
            self.logger.log_metric("confidence_score", results['confidence_score'])
            
            return results
    
    def _evaluate_clarity(self, report: str) -> float:
        """Evaluate report clarity (0-10)"""
        score = 5.0  # Base score
        
        # Check report length (should be comprehensive but not too long)
        if 1000 <= len(report) <= 5000:
            score += 1.5
        elif len(report) > 500:
            score += 0.5
        
        # Check for proper sections
        required_sections = [
            'Executive Summary', 'KPI Summary', 'Key Changes',
            'Root Cause', 'Recommendations', 'Forecast'
        ]
        sections_present = sum(1 for section in required_sections if section in report)
        score += (sections_present / len(required_sections)) * 2.0
        
        # Check for formatting (markdown headers, lists, tables)
        if '##' in report:
            score += 0.5
        if '|' in report:  # Tables
            score += 0.5
        if '-' in report or '*' in report:  # Lists
            score += 0.5
        
        return min(10.0, round(score, 1))
    
    def _evaluate_logic(self, trend_results: Dict[str, Any],
                       root_cause_results: Dict[str, Any],
                       strategy_results: Dict[str, Any]) -> float:
        """Evaluate logical consistency (0-10)"""
        score = 5.0  # Base score
        
        # Check if trends were detected
        if trend_results.get('top_trends'):
            score += 1.5
        
        # Check if root causes were identified
        if root_cause_results.get('hypotheses'):
            score += 1.5
        
        # Check if recommendations align with trends
        recommendations = strategy_results.get('recommendations', [])
        top_trends = trend_results.get('top_trends', [])
        
        if recommendations and top_trends:
            # Check if recommendations reference the KPIs from top trends
            trend_kpis = {t['kpi'] for t in top_trends}
            rec_kpis = {r['kpi'] for r in recommendations if 'kpi' in r}
            
            if trend_kpis & rec_kpis:  # Intersection
                score += 2.0
            else:
                score += 0.5
        
        # Check if risks are identified for negative trends
        risks = strategy_results.get('risks', [])
        negative_trends = [t for t in top_trends if t['growth_pct'] < -5]
        
        if negative_trends and risks:
            score += 1.0
        
        return min(10.0, round(score, 1))
    
    def _evaluate_actionability(self, strategy_results: Dict[str, Any]) -> float:
        """Evaluate actionability of recommendations (0-10)"""
        score = 5.0  # Base score
        
        recommendations = strategy_results.get('recommendations', [])
        action_plans = strategy_results.get('action_plans', [])
        
        # Check number of recommendations
        if len(recommendations) >= 3:
            score += 1.5
        elif len(recommendations) >= 1:
            score += 0.5
        
        # Check if recommendations have priority levels
        if recommendations and all('priority' in r for r in recommendations):
            score += 1.0
        
        # Check if action plans exist
        if len(action_plans) >= 2:
            score += 1.5
        elif len(action_plans) >= 1:
            score += 0.5
        
        # Check if action plans have specific actions
        if action_plans:
            has_actions = all('actions' in p and len(p['actions']) > 0 for p in action_plans)
            if has_actions:
                score += 1.0
            
            # Check if action plans have success metrics
            has_metrics = all('success_metrics' in p and len(p['success_metrics']) > 0 
                            for p in action_plans)
            if has_metrics:
                score += 1.0
        
        return min(10.0, round(score, 1))
    
    def _calculate_confidence(self, trend_results: Dict[str, Any],
                             root_cause_results: Dict[str, Any],
                             strategy_results: Dict[str, Any]) -> float:
        """Calculate overall confidence score (0-10)"""
        score = 5.0  # Base score
        
        # Higher confidence if multiple trends detected
        top_trends = trend_results.get('top_trends', [])
        if len(top_trends) >= 3:
            score += 1.5
        
        # Higher confidence if correlations are strong
        top_corr = root_cause_results.get('correlations', {}).get('top_correlations', [])
        if top_corr:
            strong_corr = [c for c in top_corr if abs(c['correlation']) >= 0.7]
            if len(strong_corr) >= 2:
                score += 1.5
            elif len(strong_corr) >= 1:
                score += 0.5
        
        # Higher confidence if hypotheses are generated
        hypotheses = root_cause_results.get('hypotheses', [])
        if len(hypotheses) >= 3:
            score += 1.0
        
        # Higher confidence if forecast confidence is medium or high
        forecast = strategy_results.get('forecast', {})
        if forecast:
            high_conf_forecasts = sum(1 for f in forecast.values() 
                                     if f.get('confidence') in ['medium', 'high'])
            if high_conf_forecasts >= 2:
                score += 1.0
        
        # Lower confidence if high volatility detected
        high_volatility = any(t.get('volatility', 0) > 30 for t in top_trends)
        if high_volatility:
            score -= 1.0
        
        return min(10.0, max(0.0, round(score, 1)))
    
    def _identify_strengths(self, results: Dict[str, Any]) -> List[str]:
        """Identify strengths in the analysis"""
        strengths = []
        
        if results['clarity_score'] >= 8.0:
            strengths.append("Excellent report clarity and structure")
        
        if results['logic_score'] >= 8.0:
            strengths.append("Strong logical consistency between trends and recommendations")
        
        if results['actionability_score'] >= 8.0:
            strengths.append("Highly actionable recommendations with clear action plans")
        
        if results['confidence_score'] >= 7.0:
            strengths.append("High confidence in analysis and predictions")
        
        if not strengths:
            strengths.append("Analysis completed successfully with acceptable quality")
        
        return strengths
    
    def _identify_weaknesses(self, results: Dict[str, Any]) -> List[str]:
        """Identify weaknesses in the analysis"""
        weaknesses = []
        
        if results['clarity_score'] < 6.0:
            weaknesses.append("Report clarity could be improved with better structure")
        
        if results['logic_score'] < 6.0:
            weaknesses.append("Logical consistency between analysis components needs strengthening")
        
        if results['actionability_score'] < 6.0:
            weaknesses.append("Recommendations lack sufficient detail or action plans")
        
        if results['confidence_score'] < 5.0:
            weaknesses.append("Low confidence due to data volatility or weak correlations")
        
        return weaknesses
    
    def _generate_suggestions(self, results: Dict[str, Any]) -> List[str]:
        """Generate improvement suggestions"""
        suggestions = []
        
        if results['clarity_score'] < 8.0:
            suggestions.append("Add more visual elements (charts, graphs) to improve clarity")
        
        if results['logic_score'] < 8.0:
            suggestions.append("Strengthen the connection between root causes and recommendations")
        
        if results['actionability_score'] < 8.0:
            suggestions.append("Provide more specific, measurable action items with clear timelines")
        
        if results['confidence_score'] < 7.0:
            suggestions.append("Collect more historical data to improve forecast confidence")
        
        if not suggestions:
            suggestions.append("Continue monitoring KPIs and refining analysis methodology")
        
        return suggestions
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Evaluation & QA Agent: Scores report clarity, logical consistency, and "
            "actionability. Identifies strengths and weaknesses, provides improvement "
            "suggestions, and calculates overall confidence scores."
        )
