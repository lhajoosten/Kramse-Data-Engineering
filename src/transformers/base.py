"""
Base transformer for data cleaning and transformation
"""
import re
import pandas as pd
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class BaseTransformer(ABC):
    """Abstract base class for all data transformers"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logger
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame"""
        raise NotImplementedError
    
    def clean_column_name(self, col_name: str) -> str:
        """Clean column names for SQL Server compatibility"""
        # Convert to string and replace problematic characters
        clean_name = str(col_name)
        
        # Replace spaces and special characters with underscores
        clean_name = re.sub(r'[^\w]', '_', clean_name)
        
        # Remove multiple underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        
        # Remove leading/trailing underscores
        clean_name = clean_name.strip('_')
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = 'col_' + clean_name
        
        # Limit length to 100 characters (SQL Server friendly)
        clean_name = clean_name[:100]
        
        return clean_name if clean_name else 'unnamed_column'
    
    def add_metadata_columns(self, df: pd.DataFrame, source_info: Dict[str, Any]) -> pd.DataFrame:
        """Add standard metadata columns"""
        df_with_metadata = df.copy()
        
        # Standard metadata
        df_with_metadata['loaded_at'] = datetime.now()
        df_with_metadata['source_file'] = source_info.get('source_file', 'unknown')
        
        return df_with_metadata
    
    def basic_data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply basic data cleaning rules"""
        clean_df = df.copy()
        
        # Clean object columns
        for col in clean_df.columns:
            if clean_df[col].dtype == 'object':
                clean_df[col] = clean_df[col].astype(str).str.strip()
                clean_df[col] = clean_df[col].replace('nan', None)
        
        return clean_df
