"""
CSV Tools - Data loading, cleaning, and validation utilities
Part of AUTOOPS AI Multi-Agent System
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import os


class CSVTools:
    """Tools for CSV data processing and validation"""
    
    def __init__(self):
        self.required_columns = ['Date', 'Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend', 'Channel']
    
    def load_csv(self, filepath: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load CSV file and return DataFrame with metadata
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            Tuple of (DataFrame, metadata_dict)
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        # Load CSV
        df = pd.read_csv(filepath)
        
        # Generate metadata
        metadata = {
            'filepath': filepath,
            'rows': len(df),
            'columns': list(df.columns),
            'file_size_kb': os.path.getsize(filepath) / 1024,
            'date_range': None
        }
        
        # Try to parse date range
        if 'Date' in df.columns:
            try:
                df['Date'] = pd.to_datetime(df['Date'])
                metadata['date_range'] = {
                    'start': df['Date'].min().strftime('%Y-%m-%d'),
                    'end': df['Date'].max().strftime('%Y-%m-%d')
                }
            except:
                pass
        
        return df, metadata
    
    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate that DataFrame has required columns and correct types
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validation results dictionary
        """
        results = {
            'valid': True,
            'missing_columns': [],
            'extra_columns': [],
            'type_issues': [],
            'messages': []
        }
        
        # Check for missing required columns
        missing = set(self.required_columns) - set(df.columns)
        if missing:
            results['valid'] = False
            results['missing_columns'] = list(missing)
            results['messages'].append(f"Missing required columns: {missing}")
        
        # Check for extra columns
        extra = set(df.columns) - set(self.required_columns)
        if extra:
            results['extra_columns'] = list(extra)
            results['messages'].append(f"Extra columns found: {extra}")
        
        # Validate data types
        if 'Date' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['Date']):
                results['type_issues'].append("Date column is not datetime type")
        
        numeric_cols = ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']
        for col in numeric_cols:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    results['type_issues'].append(f"{col} is not numeric")
                    results['valid'] = False
        
        if results['type_issues']:
            results['messages'].append(f"Type issues: {results['type_issues']}")
        
        if results['valid']:
            results['messages'].append("Schema validation passed")
        
        return results
    
    def clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Clean data: handle nulls, convert types, remove duplicates
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Tuple of (cleaned DataFrame, cleaning report)
        """
        df_clean = df.copy()
        report = {
            'nulls_found': {},
            'nulls_filled': {},
            'duplicates_removed': 0,
            'rows_before': len(df),
            'rows_after': 0,
            'actions': []
        }
        
        # Check for nulls
        null_counts = df_clean.isnull().sum()
        report['nulls_found'] = null_counts[null_counts > 0].to_dict()
        
        # Handle nulls in numeric columns (forward fill, then backward fill)
        numeric_cols = ['Revenue', 'Customers', 'Conversion_Rate', 'Marketing_Spend']
        for col in numeric_cols:
            if col in df_clean.columns:
                nulls_before = df_clean[col].isnull().sum()
                if nulls_before > 0:
                    df_clean[col] = df_clean[col].ffill().bfill()
                    report['nulls_filled'][col] = nulls_before
                    report['actions'].append(f"Filled {nulls_before} nulls in {col}")
        
        # Handle nulls in categorical columns (fill with 'Unknown')
        if 'Channel' in df_clean.columns:
            nulls_before = df_clean['Channel'].isnull().sum()
            if nulls_before > 0:
                df_clean['Channel'] = df_clean['Channel'].fillna('Unknown')
                report['nulls_filled']['Channel'] = nulls_before
                report['actions'].append(f"Filled {nulls_before} nulls in Channel")
        
        # Remove duplicates based on Date
        if 'Date' in df_clean.columns:
            before = len(df_clean)
            df_clean = df_clean.drop_duplicates(subset=['Date'], keep='first')
            duplicates = before - len(df_clean)
            if duplicates > 0:
                report['duplicates_removed'] = duplicates
                report['actions'].append(f"Removed {duplicates} duplicate rows")
        
        # Sort by date
        if 'Date' in df_clean.columns:
            df_clean = df_clean.sort_values('Date').reset_index(drop=True)
            report['actions'].append("Sorted data by Date")
        
        report['rows_after'] = len(df_clean)
        
        return df_clean, report
    
    def get_data_quality_score(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate data quality metrics
        
        Args:
            df: DataFrame to assess
            
        Returns:
            Quality metrics dictionary
        """
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        
        quality = {
            'completeness': round((1 - null_cells / total_cells) * 100, 2),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_cells': int(null_cells),
            'null_percentage': round((null_cells / total_cells) * 100, 2),
            'quality_grade': 'A'
        }
        
        # Assign quality grade
        if quality['completeness'] >= 95:
            quality['quality_grade'] = 'A'
        elif quality['completeness'] >= 85:
            quality['quality_grade'] = 'B'
        elif quality['completeness'] >= 70:
            quality['quality_grade'] = 'C'
        else:
            quality['quality_grade'] = 'D'
        
        return quality


# Convenience functions for direct use
def load_and_clean_csv(filepath: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Load and clean CSV in one step
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Tuple of (cleaned DataFrame, full report)
    """
    tools = CSVTools()
    
    # Load
    df, load_metadata = tools.load_csv(filepath)
    
    # Validate
    validation = tools.validate_schema(df)
    
    # Clean
    df_clean, clean_report = tools.clean_data(df)
    
    # Quality score
    quality = tools.get_data_quality_score(df_clean)
    
    # Combined report
    report = {
        'load_metadata': load_metadata,
        'validation': validation,
        'cleaning': clean_report,
        'quality': quality
    }
    
    return df_clean, report
