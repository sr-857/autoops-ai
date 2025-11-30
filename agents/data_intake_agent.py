"""
Data Intake Agent - Loads, validates, and cleans CSV data
Part of AUTOOPS AI Multi-Agent System
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.csv_tools import CSVTools, load_and_clean_csv
from tools.logging_tools import ObservabilityLogger, AgentTimer
from typing import Dict, Any, Tuple
import pandas as pd


class DataIntakeAgent:
    """Agent responsible for data loading, validation, and cleaning"""
    
    def __init__(self, logger: ObservabilityLogger):
        """
        Initialize Data Intake Agent
        
        Args:
            logger: ObservabilityLogger instance
        """
        self.name = "DataIntakeAgent"
        self.logger = logger
        self.csv_tools = CSVTools()
    
    def execute(self, filepath: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Execute data intake process
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            Tuple of (cleaned DataFrame, intake report)
        """
        with AgentTimer(self.logger, self.name):
            self.logger.log_tool_usage(self.name, "csv_tools.load_and_clean_csv", 
                                      {"filepath": filepath})
            
            # Load and clean data
            df_clean, report = load_and_clean_csv(filepath)
            
            # Log data quality insights
            quality = report['quality']
            self.logger.log_insight(
                f"Data quality score: {quality['quality_grade']} "
                f"({quality['completeness']}% complete)",
                category="data_quality"
            )
            
            # Log validation results
            validation = report['validation']
            if validation['valid']:
                self.logger.log_insight("Schema validation passed", category="data_quality")
            else:
                self.logger.logger.warning(f"Schema issues: {validation['messages']}")
            
            # Log cleaning actions
            cleaning = report['cleaning']
            for action in cleaning['actions']:
                self.logger.log_insight(action, category="data_cleaning")
            
            # Create summary for next agents
            intake_summary = {
                'rows': len(df_clean),
                'columns': list(df_clean.columns),
                'date_range': report['load_metadata']['date_range'],
                'quality_grade': quality['quality_grade'],
                'completeness': quality['completeness']
            }
            
            self.logger.log_metric("data_rows_processed", len(df_clean))
            self.logger.log_metric("data_quality_score", quality['completeness'])
            
            return df_clean, {
                'summary': intake_summary,
                'full_report': report
            }
    
    def get_description(self) -> str:
        """Get agent description"""
        return (
            "Data Intake Agent: Responsible for loading CSV files, "
            "validating data schema, cleaning data (handling nulls, duplicates), "
            "and assessing data quality."
        )
