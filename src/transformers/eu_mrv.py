"""
EU MRV data transformer
"""
import pandas as pd
from typing import Dict, List
from .base import BaseTransformer

class EUMRVTransformer(BaseTransformer):
    """Transformer for EU MRV shipping data"""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform EU MRV data with all columns"""
        try:
            self.logger.info(f"Transforming {len(df)} EU MRV records with {len(df.columns)} columns")
            
            # Keep all columns
            self.logger.info(f"Processing all {len(df.columns)} columns from EU MRV data")
            
            # Clean column names for SQL Server compatibility
            original_columns = df.columns.tolist()
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Log column mapping (sample)
            sample_mapping = dict(list(zip(original_columns[:10], df.columns[:10])))
            self.logger.info(f"Sample column mapping: {sample_mapping}...")
            
            # Basic data cleaning
            df = df.fillna('')  # Replace NaN with empty strings
            
            # Add EU MRV specific metadata
            df['processed_date'] = pd.Timestamp.now()
            df['source_file'] = 'eu_mrv_shipping'
            df['total_columns_in_source'] = len(original_columns)
            
            self.logger.info(f"EU MRV data transformed: {len(df)} records, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to transform EU MRV data: {e}")
            raise
