"""
Specialized loader for EU MRV data with many columns
"""
import os
import pandas as pd
import sqlalchemy as sa
from .base import BaseLoader

class EUMRVLoader(BaseLoader):
    """Specialized loader for EU MRV data with record-by-record processing"""
    
    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load EU MRV data record by record for all columns"""
        try:
            if not self.validate_data(df):
                return 0
            
            self.logger.info(f"Loading {len(df)} EU MRV records with {len(df.columns)} columns to {table_name} one by one")
            
            # Create specialized engine for target database
            target_db = self.config.get('target_db', 'Kramse_RAW')
            engine = self._create_target_engine(target_db)
            
            # Load record by record to avoid parameter limits
            total_loaded = 0
            failed_records = 0
            
            for i, (index, row) in enumerate(df.iterrows()):
                try:
                    row_df = pd.DataFrame([row])
                    row_df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='replace' if i == 0 else 'append',
                        index=False,
                        method=None
                    )
                    total_loaded += 1
                    
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Loaded {i + 1} records so far...")
                        
                except Exception as record_error:
                    failed_records += 1
                    self.logger.warning(f"Failed to load record {i + 1}: {record_error}")
                    continue
            
            self.logger.info(f"EU MRV loading completed: {total_loaded}/{len(df)} records loaded ({failed_records} failed)")
            return total_loaded
            
        except Exception as e:
            self.logger.error(f"Failed to load EU MRV data: {e}")
            return 0
    
    def _create_target_engine(self, target_db: str) -> sa.Engine:
        """Create engine for specific target database"""
        server = os.getenv('DB_SERVER', 'localhost')
        username = os.getenv('DB_USERNAME', 'sa')
        password = os.getenv('DB_PASSWORD')
        driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{target_db}?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
        return sa.create_engine(connection_string)
