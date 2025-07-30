"""
Standard batch loader for regular data
"""
import pandas as pd
from .base import BaseLoader

class BatchLoader(BaseLoader):
    """Standard batch loader for regular datasets"""
    
    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame in batches"""
        try:
            if not self.validate_data(df):
                return 0
            
            # Get engine for raw database
            engine = self.db_manager.get_engine('Kramse_RAW')
            
            batch_size = self.config.get('batch_size', 500)
            self.logger.info("Loading %d records to %s", len(df), table_name)
            
            if len(df) > batch_size:
                return self._load_in_batches(df, table_name, batch_size, engine)
            else:
                return self._load_single_batch(df, table_name, engine)
                
        except Exception as e:
            self.logger.error("Failed to load data to %s: %s", table_name, e)
            raise
    
    def _load_in_batches(self, df: pd.DataFrame, table_name: str, batch_size: int, engine) -> int:
        """Load data in multiple batches"""
        self.logger.info("Using batch processing with batch size: %d", batch_size)
        
        total_loaded = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(df) + batch_size - 1) // batch_size
            
            self.logger.info("Processing batch %d/%d (%d records)", batch_num, total_batches, len(batch_df))
            
            # First batch replaces, subsequent batches append
            if_exists_batch = 'replace' if i == 0 else 'append'
            
            batch_df.to_sql(
                table_name,
                engine,
                if_exists=if_exists_batch,
                index=False,
                method='multi',
                chunksize=100
            )
            total_loaded += len(batch_df)
        
        self.logger.info("Batch loading completed: %d total records loaded", total_loaded)
        return total_loaded
    
    def _load_single_batch(self, df: pd.DataFrame, table_name: str, engine) -> int:
        """Load data in single operation"""
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=250
        )
        
        self.logger.info("Successfully loaded %d records to %s", len(df), table_name)
        return len(df)
