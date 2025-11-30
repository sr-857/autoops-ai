"""
Statistical Tools - Trend detection, anomaly detection, and correlation analysis
Part of AUTOOPS AI Multi-Agent System
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
from scipy import stats


class StatsTools:
    """Tools for statistical analysis and trend detection"""
    
    def __init__(self):
        self.z_threshold = 3.0  # Z-score threshold for anomaly detection
        self.iqr_multiplier = 1.5  # IQR multiplier for outlier detection
    
    def calculate_basic_stats(self, df: pd.DataFrame, column: str) -> Dict[str, float]:
        """
        Calculate basic statistical measures for a column
        
        Args:
            df: DataFrame
            column: Column name
            
        Returns:
            Dictionary of statistics
        """
        if column not in df.columns:
            return {}
        
        data = df[column].dropna()
        
        return {
            'mean': float(data.mean()),
            'median': float(data.median()),
            'std': float(data.std()),
            'min': float(data.min()),
            'max': float(data.max()),
            'q25': float(data.quantile(0.25)),
            'q75': float(data.quantile(0.75)),
            'count': int(len(data))
        }
    
    def detect_trend(self, df: pd.DataFrame, column: str, window: int = 7) -> Dict[str, Any]:
        """
        Detect trends using moving averages and growth rates
        
        Args:
            df: DataFrame with Date column
            column: Column to analyze
            window: Moving average window size
            
        Returns:
            Trend analysis results
        """
        if column not in df.columns or 'Date' not in df.columns:
            return {}
        
        df_sorted = df.sort_values('Date').copy()
        values = df_sorted[column].values
        
        # Calculate moving average
        ma = pd.Series(values).rolling(window=window, min_periods=1).mean()
        
        # Calculate overall growth rate
        if len(values) > 1:
            first_val = values[0] if values[0] != 0 else 0.01
            last_val = values[-1]
            total_growth = ((last_val - first_val) / abs(first_val)) * 100
        else:
            total_growth = 0
        
        # Calculate period-over-period changes
        pct_changes = pd.Series(values).pct_change().fillna(0) * 100
        
        # Determine trend direction
        if total_growth > 5:
            trend_direction = 'upward'
        elif total_growth < -5:
            trend_direction = 'downward'
        else:
            trend_direction = 'stable'
        
        # Calculate volatility
        volatility = float(pct_changes.std())
        
        return {
            'column': column,
            'total_growth_pct': round(total_growth, 2),
            'trend_direction': trend_direction,
            'avg_period_change_pct': round(pct_changes.mean(), 2),
            'volatility': round(volatility, 2),
            'moving_average': ma.tolist(),
            'recent_trend': 'increasing' if ma.iloc[-1] > ma.iloc[-min(7, len(ma))] else 'decreasing'
        }
    
    def detect_anomalies(self, df: pd.DataFrame, column: str, method: str = 'zscore') -> Dict[str, Any]:
        """
        Detect anomalies using statistical methods
        
        Args:
            df: DataFrame
            column: Column to analyze
            method: 'zscore' or 'iqr'
            
        Returns:
            Anomaly detection results
        """
        if column not in df.columns:
            return {}
        
        data = df[column].dropna()
        anomalies = []
        
        if method == 'zscore':
            # Z-score method
            z_scores = np.abs(stats.zscore(data))
            anomaly_indices = np.where(z_scores > self.z_threshold)[0]
            
            for idx in anomaly_indices:
                actual_idx = data.index[idx]
                anomalies.append({
                    'index': int(actual_idx),
                    'value': float(data.iloc[idx]),
                    'z_score': float(z_scores[idx]),
                    'date': df.loc[actual_idx, 'Date'].strftime('%Y-%m-%d') if 'Date' in df.columns else None
                })
        
        elif method == 'iqr':
            # IQR method
            q1 = data.quantile(0.25)
            q3 = data.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - self.iqr_multiplier * iqr
            upper_bound = q3 + self.iqr_multiplier * iqr
            
            anomaly_mask = (data < lower_bound) | (data > upper_bound)
            anomaly_indices = np.where(anomaly_mask)[0]
            
            for idx in anomaly_indices:
                actual_idx = data.index[idx]
                anomalies.append({
                    'index': int(actual_idx),
                    'value': float(data.iloc[idx]),
                    'lower_bound': float(lower_bound),
                    'upper_bound': float(upper_bound),
                    'date': df.loc[actual_idx, 'Date'].strftime('%Y-%m-%d') if 'Date' in df.columns else None
                })
        
        return {
            'column': column,
            'method': method,
            'anomalies_found': len(anomalies),
            'anomalies': anomalies[:10],  # Limit to top 10
            'anomaly_rate': round(len(anomalies) / len(data) * 100, 2)
        }
    
    def calculate_correlation(self, df: pd.DataFrame, col1: str, col2: str) -> Dict[str, Any]:
        """
        Calculate correlation between two columns
        
        Args:
            df: DataFrame
            col1: First column
            col2: Second column
            
        Returns:
            Correlation analysis
        """
        if col1 not in df.columns or col2 not in df.columns:
            return {}
        
        # Drop rows with nulls in either column
        df_clean = df[[col1, col2]].dropna()
        
        if len(df_clean) < 2:
            return {'error': 'Insufficient data for correlation'}
        
        # Pearson correlation
        pearson_corr, pearson_p = stats.pearsonr(df_clean[col1], df_clean[col2])
        
        # Spearman correlation (rank-based, robust to outliers)
        spearman_corr, spearman_p = stats.spearmanr(df_clean[col1], df_clean[col2])
        
        # Interpret correlation strength
        abs_corr = abs(pearson_corr)
        if abs_corr >= 0.7:
            strength = 'strong'
        elif abs_corr >= 0.4:
            strength = 'moderate'
        elif abs_corr >= 0.2:
            strength = 'weak'
        else:
            strength = 'very weak'
        
        direction = 'positive' if pearson_corr > 0 else 'negative'
        
        return {
            'col1': col1,
            'col2': col2,
            'pearson_correlation': round(pearson_corr, 3),
            'pearson_p_value': round(pearson_p, 4),
            'spearman_correlation': round(spearman_corr, 3),
            'spearman_p_value': round(spearman_p, 4),
            'strength': strength,
            'direction': direction,
            'significant': pearson_p < 0.05
        }
    
    def correlation_matrix(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
        """
        Calculate correlation matrix for multiple columns
        
        Args:
            df: DataFrame
            columns: List of column names
            
        Returns:
            Correlation matrix and insights
        """
        # Filter to numeric columns that exist
        valid_cols = [col for col in columns if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
        
        if len(valid_cols) < 2:
            return {'error': 'Need at least 2 numeric columns'}
        
        # Calculate correlation matrix
        corr_matrix = df[valid_cols].corr()
        
        # Find strongest correlations (excluding diagonal)
        correlations = []
        for i in range(len(valid_cols)):
            for j in range(i + 1, len(valid_cols)):
                correlations.append({
                    'col1': valid_cols[i],
                    'col2': valid_cols[j],
                    'correlation': round(corr_matrix.iloc[i, j], 3)
                })
        
        # Sort by absolute correlation
        correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)
        
        return {
            'matrix': corr_matrix.to_dict(),
            'top_correlations': correlations[:5],
            'columns_analyzed': valid_cols
        }
    
    def calculate_growth_rate(self, df: pd.DataFrame, column: str, periods: int = 7) -> Dict[str, Any]:
        """
        Calculate growth rate over specified periods
        
        Args:
            df: DataFrame with Date column
            column: Column to analyze
            periods: Number of periods to compare
            
        Returns:
            Growth rate analysis
        """
        if column not in df.columns or 'Date' not in df.columns:
            return {}
        
        df_sorted = df.sort_values('Date').copy()
        values = df_sorted[column].values
        
        if len(values) < periods + 1:
            return {'error': f'Need at least {periods + 1} data points'}
        
        # Recent period average
        recent_avg = np.mean(values[-periods:])
        
        # Previous period average
        previous_avg = np.mean(values[-2*periods:-periods])
        
        # Calculate growth
        if previous_avg != 0:
            growth_rate = ((recent_avg - previous_avg) / abs(previous_avg)) * 100
        else:
            growth_rate = 0
        
        # Week-over-week changes
        wow_changes = []
        for i in range(len(values) - periods):
            current = np.mean(values[i+periods:i+2*periods])
            previous = np.mean(values[i:i+periods])
            if previous != 0:
                change = ((current - previous) / abs(previous)) * 100
                wow_changes.append(change)
        
        return {
            'column': column,
            'periods': periods,
            'recent_average': round(recent_avg, 2),
            'previous_average': round(previous_avg, 2),
            'growth_rate_pct': round(growth_rate, 2),
            'avg_period_growth': round(np.mean(wow_changes), 2) if wow_changes else 0,
            'growth_volatility': round(np.std(wow_changes), 2) if wow_changes else 0
        }


# Convenience function
def analyze_kpis(df: pd.DataFrame, kpi_columns: List[str]) -> Dict[str, Any]:
    """
    Comprehensive KPI analysis
    
    Args:
        df: DataFrame
        kpi_columns: List of KPI column names
        
    Returns:
        Complete analysis results
    """
    tools = StatsTools()
    results = {}
    
    for col in kpi_columns:
        if col in df.columns:
            results[col] = {
                'basic_stats': tools.calculate_basic_stats(df, col),
                'trend': tools.detect_trend(df, col),
                'anomalies': tools.detect_anomalies(df, col),
                'growth': tools.calculate_growth_rate(df, col)
            }
    
    # Add correlation matrix
    results['correlations'] = tools.correlation_matrix(df, kpi_columns)
    
    return results
