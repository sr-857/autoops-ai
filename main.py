"""
AUTOOPS AI - Multi-Agent System for Business Intelligence
Main orchestration module

This system transforms raw business data into executive decisions through
a pipeline of specialized AI agents.
"""

import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import tools
from tools.logging_tools import ObservabilityLogger
from tools.memory_store import MemoryStore

# Import agents
from agents.data_intake_agent import DataIntakeAgent
from agents.trend_agent import TrendDetectionAgent
from agents.root_cause_agent import RootCauseAgent
from agents.memory_agent import MemoryAgent
from agents.strategy_agent import StrategyAgent
from agents.reporting_agent import ReportingAgent
from agents.evaluation_agent import EvaluationAgent


class AutoOpsAI:
    """Main orchestrator for the multi-agent system"""
    
    def __init__(self, log_file: str = "logs/system.log", 
                 memory_file: str = "memory/long_term_memory.json"):
        """
        Initialize AUTOOPS AI system
        
        Args:
            log_file: Path to log file
            memory_file: Path to memory JSON file
        """
        # Initialize logger
        self.logger = ObservabilityLogger(log_file)
        
        # Initialize agents
        self.logger.logger.info("Initializing agents...")
        
        self.data_intake_agent = DataIntakeAgent(self.logger)
        self.trend_agent = TrendDetectionAgent(self.logger)
        self.root_cause_agent = RootCauseAgent(self.logger)
        self.memory_agent = MemoryAgent(self.logger, memory_file)
        self.strategy_agent = StrategyAgent(self.logger)
        self.reporting_agent = ReportingAgent(self.logger)
        self.evaluation_agent = EvaluationAgent(self.logger)
        
        self.logger.logger.info("✓ All agents initialized successfully")
        
        # Store results from each agent
        self.results = {}
    
    def run(self, input_csv: str, output_report: str = "output/executive_report.md") -> dict:
        """
        Execute the complete multi-agent pipeline
        
        Args:
            input_csv: Path to input CSV file
            output_report: Path to output report file
            
        Returns:
            Dictionary with all results
        """
        self.logger.logger.info("=" * 70)
        self.logger.logger.info("AUTOOPS AI - Multi-Agent System Starting")
        self.logger.logger.info("=" * 70)
        
        try:
            # AGENT 1: Data Intake
            self.logger.logger.info("\n[1/7] Executing Data Intake Agent...")
            df_clean, intake_results = self.data_intake_agent.execute(input_csv)
            self.results['intake'] = intake_results
            self.logger.logger.info(f"✓ Processed {len(df_clean)} rows of data")
            
            # AGENT 2: Trend Detection
            self.logger.logger.info("\n[2/7] Executing Trend Detection Agent...")
            trend_results = self.trend_agent.execute(df_clean)
            self.results['trends'] = trend_results
            self.logger.logger.info(f"✓ Detected {len(trend_results.get('top_trends', []))} key trends")
            
            # AGENT 3: Root Cause Analysis
            self.logger.logger.info("\n[3/7] Executing Root Cause Analysis Agent...")
            root_cause_results = self.root_cause_agent.execute(df_clean, trend_results)
            self.results['root_cause'] = root_cause_results
            self.logger.logger.info(f"✓ Generated {len(root_cause_results.get('hypotheses', []))} hypotheses")
            
            # AGENT 4: Memory Management
            self.logger.logger.info("\n[4/7] Executing Memory Agent...")
            memory_results = self.memory_agent.execute(df_clean, trend_results, root_cause_results)
            self.results['memory'] = memory_results
            self.logger.logger.info(f"✓ Session stored: {memory_results.get('session_id', 'N/A')}")
            
            # AGENT 5: Strategy Generation
            self.logger.logger.info("\n[5/7] Executing Strategy Agent...")
            strategy_results = self.strategy_agent.execute(
                df_clean, trend_results, root_cause_results, memory_results
            )
            self.results['strategy'] = strategy_results
            self.logger.logger.info(f"✓ Generated {len(strategy_results.get('recommendations', []))} recommendations")
            
            # AGENT 6: Executive Reporting
            self.logger.logger.info("\n[6/7] Executing Executive Reporting Agent...")
            report = self.reporting_agent.execute(
                df_clean, intake_results, trend_results, root_cause_results,
                memory_results, strategy_results
            )
            self.results['report'] = report
            
            # Save report to file
            os.makedirs(os.path.dirname(output_report), exist_ok=True)
            with open(output_report, 'w') as f:
                f.write(report)
            self.logger.logger.info(f"✓ Report saved to: {output_report}")
            
            # AGENT 7: Evaluation & QA
            self.logger.logger.info("\n[7/7] Executing Evaluation Agent...")
            evaluation_results = self.evaluation_agent.execute(
                report, trend_results, root_cause_results, strategy_results
            )
            self.results['evaluation'] = evaluation_results
            self.logger.logger.info(f"✓ Overall Quality Score: {evaluation_results['overall_score']}/10")
            self.logger.logger.info(f"✓ Confidence Score: {evaluation_results['confidence_score']}/10")
            
            # Append evaluation to report
            self._append_evaluation_to_report(output_report, evaluation_results)
            
            # Log summary
            self.logger.log_summary()
            
            # Save trace
            trace_file = output_report.replace('.md', '_trace.json')
            self.logger.save_trace(trace_file)
            
            self.logger.logger.info("\n" + "=" * 70)
            self.logger.logger.info("AUTOOPS AI - Execution Completed Successfully")
            self.logger.logger.info("=" * 70)
            
            return {
                'success': True,
                'report_path': output_report,
                'trace_path': trace_file,
                'results': self.results
            }
            
        except Exception as e:
            self.logger.logger.error(f"System error: {e}")
            import traceback
            self.logger.logger.error(traceback.format_exc())
            
            return {
                'success': False,
                'error': str(e),
                'results': self.results
            }
    
    def _append_evaluation_to_report(self, report_path: str, evaluation: dict):
        """Append evaluation section to report"""
        with open(report_path, 'a') as f:
            f.write("\n\n---\n\n")
            f.write("## 🎯 System Evaluation\n\n")
            f.write("**Quality Scores:**\n\n")
            f.write(f"- Clarity: {evaluation['clarity_score']}/10\n")
            f.write(f"- Logical Consistency: {evaluation['logic_score']}/10\n")
            f.write(f"- Actionability: {evaluation['actionability_score']}/10\n")
            f.write(f"- **Overall Score: {evaluation['overall_score']}/10**\n")
            f.write(f"- **Confidence Score: {evaluation['confidence_score']}/10**\n\n")
            
            if evaluation['strengths']:
                f.write("**Strengths:**\n\n")
                for strength in evaluation['strengths']:
                    f.write(f"- ✅ {strength}\n")
                f.write("\n")
            
            if evaluation['weaknesses']:
                f.write("**Areas for Improvement:**\n\n")
                for weakness in evaluation['weaknesses']:
                    f.write(f"- ⚠️ {weakness}\n")
                f.write("\n")
            
            if evaluation['improvement_suggestions']:
                f.write("**Suggestions:**\n\n")
                for suggestion in evaluation['improvement_suggestions']:
                    f.write(f"- 💡 {suggestion}\n")
                f.write("\n")
            
            f.write("---\n\n")
            f.write(f"*Report generated by AUTOOPS AI on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='AUTOOPS AI - Multi-Agent Business Intelligence System'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='datasets/sample_sales_data.csv',
        help='Path to input CSV file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='output/executive_report.md',
        help='Path to output report file'
    )
    parser.add_argument(
        '--log',
        type=str,
        default='logs/system.log',
        help='Path to log file'
    )
    parser.add_argument(
        '--memory',
        type=str,
        default='memory/long_term_memory.json',
        help='Path to memory file'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print("\n" + "=" * 70)
    print("  AUTOOPS AI - Multi-Agent System for Business Intelligence")
    print("  Transforming Raw Data into Executive Decisions")
    print("=" * 70 + "\n")
    
    # Initialize and run system
    system = AutoOpsAI(log_file=args.log, memory_file=args.memory)
    result = system.run(input_csv=args.input, output_report=args.output)
    
    # Print results
    if result['success']:
        print("\n✅ SUCCESS!")
        print(f"\n📄 Executive Report: {result['report_path']}")
        print(f"📊 Session Trace: {result['trace_path']}")
        print(f"📝 System Logs: {args.log}")
        
        eval_results = result['results'].get('evaluation', {})
        print(f"\n🎯 Quality Score: {eval_results.get('overall_score', 'N/A')}/10")
        print(f"🎯 Confidence Score: {eval_results.get('confidence_score', 'N/A')}/10")
    else:
        print("\n❌ FAILED!")
        print(f"Error: {result['error']}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
