"""
Memory Store - Long-term memory management for multi-agent system
Part of AUTOOPS AI Multi-Agent System
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import threading


class MemoryStore:
    """Thread-safe long-term memory storage using JSON"""
    
    def __init__(self, memory_file: str):
        """
        Initialize memory store
        
        Args:
            memory_file: Path to JSON memory file
        """
        self.memory_file = memory_file
        self.lock = threading.Lock()
        
        # Initialize memory file if it doesn't exist
        if not os.path.exists(memory_file):
            self._initialize_memory()
    
    def _initialize_memory(self):
        """Create initial memory structure"""
        initial_memory = {
            'sessions': [],
            'kpi_history': {},
            'insights': [],
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'total_sessions': 0,
                'last_updated': None
            }
        }
        
        os.makedirs(os.path.dirname(self.memory_file), exist_ok=True)
        
        with open(self.memory_file, 'w') as f:
            json.dump(initial_memory, f, indent=2)
    
    def _load_memory(self) -> Dict[str, Any]:
        """Load memory from file"""
        with open(self.memory_file, 'r') as f:
            return json.load(f)
    
    def _save_memory(self, memory: Dict[str, Any]):
        """Save memory to file"""
        memory['metadata']['last_updated'] = datetime.now().isoformat()
        
        with open(self.memory_file, 'w') as f:
            json.dump(memory, f, indent=2)
    
    def store_session(self, session_data: Dict[str, Any]) -> str:
        """
        Store a complete session
        
        Args:
            session_data: Session information
            
        Returns:
            Session ID
        """
        with self.lock:
            memory = self._load_memory()
            
            # Generate session ID
            session_id = f"session_{len(memory['sessions']) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Add session
            session = {
                'session_id': session_id,
                'timestamp': datetime.now().isoformat(),
                'data': session_data
            }
            
            memory['sessions'].append(session)
            memory['metadata']['total_sessions'] += 1
            
            self._save_memory(memory)
            
            return session_id
    
    def store_kpi_snapshot(self, date: str, kpis: Dict[str, float]):
        """
        Store KPI snapshot for a specific date
        
        Args:
            date: Date string (YYYY-MM-DD)
            kpis: Dictionary of KPI values
        """
        with self.lock:
            memory = self._load_memory()
            
            if date not in memory['kpi_history']:
                memory['kpi_history'][date] = {}
            
            memory['kpi_history'][date].update(kpis)
            
            self._save_memory(memory)
    
    def store_kpi_snapshots_batch(self, snapshots: Dict[str, Dict[str, float]]):
        """
        Store multiple KPI snapshots at once (more efficient than individual calls)
        
        Args:
            snapshots: Dictionary of date -> KPI values
        """
        with self.lock:
            memory = self._load_memory()
            
            for date, kpis in snapshots.items():
                if date not in memory['kpi_history']:
                    memory['kpi_history'][date] = {}
                memory['kpi_history'][date].update(kpis)
            
            self._save_memory(memory)
    
    def store_insight(self, insight: Dict[str, Any]):
        """
        Store a business insight
        
        Args:
            insight: Insight dictionary
        """
        with self.lock:
            memory = self._load_memory()
            
            insight['timestamp'] = datetime.now().isoformat()
            memory['insights'].append(insight)
            
            # Keep only last 100 insights
            if len(memory['insights']) > 100:
                memory['insights'] = memory['insights'][-100:]
            
            self._save_memory(memory)
    
    def get_recent_sessions(self, n: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieve recent sessions
        
        Args:
            n: Number of sessions to retrieve
            
        Returns:
            List of recent sessions
        """
        with self.lock:
            memory = self._load_memory()
            return memory['sessions'][-n:]
    
    def get_kpi_history(self, kpi_name: str, days: int = 30) -> Dict[str, float]:
        """
        Get historical values for a specific KPI
        
        Args:
            kpi_name: Name of the KPI
            days: Number of days to retrieve
            
        Returns:
            Dictionary of date -> value
        """
        with self.lock:
            memory = self._load_memory()
            
            history = {}
            for date, kpis in memory['kpi_history'].items():
                if kpi_name in kpis:
                    history[date] = kpis[kpi_name]
            
            # Sort by date and return most recent
            sorted_dates = sorted(history.keys(), reverse=True)[:days]
            return {date: history[date] for date in sorted_dates}
    
    def get_insights(self, category: Optional[str] = None, n: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieve stored insights
        
        Args:
            category: Optional category filter
            n: Number of insights to retrieve
            
        Returns:
            List of insights
        """
        with self.lock:
            memory = self._load_memory()
            
            insights = memory['insights']
            
            if category:
                insights = [i for i in insights if i.get('category') == category]
            
            return insights[-n:]
    
    def compare_with_history(self, current_kpis: Dict[str, float], lookback_days: int = 30) -> Dict[str, Any]:
        """
        Compare current KPIs with historical averages
        
        Args:
            current_kpis: Current KPI values
            lookback_days: Days to look back
            
        Returns:
            Comparison results
        """
        with self.lock:
            memory = self._load_memory()
            
            comparisons = {}
            
            for kpi_name, current_value in current_kpis.items():
                # Get history inline to avoid nested lock
                history = {}
                for date, kpis in memory['kpi_history'].items():
                    if kpi_name in kpis:
                        history[date] = kpis[kpi_name]
                
                # Sort by date and get most recent
                if history:
                    sorted_dates = sorted(history.keys(), reverse=True)[:lookback_days]
                    recent_history = {date: history[date] for date in sorted_dates}
                    historical_values = list(recent_history.values())
                    avg_historical = sum(historical_values) / len(historical_values)
                    
                    change = current_value - avg_historical
                    change_pct = (change / avg_historical * 100) if avg_historical != 0 else 0
                    
                    comparisons[kpi_name] = {
                        'current': current_value,
                        'historical_avg': round(avg_historical, 2),
                        'change': round(change, 2),
                        'change_pct': round(change_pct, 2),
                        'data_points': len(historical_values)
                    }
                else:
                    comparisons[kpi_name] = {
                        'current': current_value,
                        'historical_avg': None,
                        'change': None,
                        'change_pct': None,
                        'data_points': 0
                    }
            
            return comparisons
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get memory store statistics
        
        Returns:
            Statistics dictionary
        """
        with self.lock:
            memory = self._load_memory()
            
            return {
                'total_sessions': memory['metadata']['total_sessions'],
                'total_insights': len(memory['insights']),
                'kpi_dates_tracked': len(memory['kpi_history']),
                'created_at': memory['metadata']['created_at'],
                'last_updated': memory['metadata']['last_updated']
            }
    
    def clear_old_data(self, days_to_keep: int = 90):
        """
        Clear data older than specified days
        
        Args:
            days_to_keep: Number of days to retain
        """
        with self.lock:
            memory = self._load_memory()
            
            cutoff_date = datetime.now()
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_to_keep)
            cutoff_str = cutoff_date.strftime('%Y-%m-%d')
            
            # Filter KPI history
            memory['kpi_history'] = {
                date: kpis for date, kpis in memory['kpi_history'].items()
                if date >= cutoff_str
            }
            
            # Filter sessions
            memory['sessions'] = [
                session for session in memory['sessions']
                if session['timestamp'][:10] >= cutoff_str
            ]
            
            # Filter insights
            memory['insights'] = [
                insight for insight in memory['insights']
                if insight['timestamp'][:10] >= cutoff_str
            ]
            
            self._save_memory(memory)
