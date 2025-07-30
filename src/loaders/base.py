"""
Base loader for database operations
"""
import pandas as pd
import sqlalchemy as sa
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class BaseLoader(ABC):
    """Abstract base class for all data loaders"""
    
    def __init__(self, config_path: str = None):
        from ..database import DatabaseManager
        self.config_path = config_path or "config/database.yaml"
        self.db_manager = DatabaseManager(config_path)
        self.config = {}
        self.logger = logger
    
    @abstractmethod
    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame to database table"""
        raise NotImplementedError
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate data before loading"""
        if df.empty:
            self.logger.warning("DataFrame is empty")
            return False
        
        if df.isna().all().all():
            self.logger.warning("DataFrame contains only null values")
            return False
        
        return True
