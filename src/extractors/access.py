"""
MS Access database extractor
"""
import os
import pandas as pd
import pyodbc
from pathlib import Path
from typing import Dict
from .base import BaseExtractor

class AccessExtractor(BaseExtractor):
    """Extractor for MS Access database data"""
    
    def __init__(self, file_path: str = None):
        config = {
            'file_path': file_path or os.getenv('ACCESSDB_FILE', 'data/KramseTPS v7.mdb'),
        }
        super().__init__(config)
    
    def validate_source(self) -> bool:
        """Check if Access database file exists"""
        file_path = self.source_config['file_path']
        exists = Path(file_path).exists()
        if not exists:
            self.logger.error(f"Access database file not found: {file_path}")
        return exists
    
    def extract(self, source_path: str = None) -> Dict[str, pd.DataFrame]:
        """Extract all tables from MS Access database"""
        try:
            # Use provided path or default from config
            if source_path:
                self.source_config['file_path'] = source_path
                
            if not self.validate_source():
                raise FileNotFoundError(f"Source file not accessible")
            
            file_path = self.source_config['file_path']
            self.logger.info(f"Extracting Access data from: {file_path}")
            
            # Connection string for MS Access
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                f'DBQ={Path(file_path).absolute()};'
            )
            
            conn = pyodbc.connect(conn_str)
            
            # Get all tables
            cursor = conn.cursor()
            tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
            self.logger.info(f"Found {len(tables)} tables: {tables}")
            
            # Extract all tables
            access_data = {}
            for table_name in tables:
                try:
                    query = f"SELECT * FROM [{table_name}]"
                    df = pd.read_sql(query, conn)
                    
                    # Basic data cleaning
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace('nan', None)
                    
                    access_data[table_name] = df
                    self.logger.info(f"Extracted {len(df)} records from: {table_name}")
                    
                except Exception as e:
                    self.logger.warning(f"Could not extract table {table_name}: {e}")
            
            conn.close()
            return access_data
            
        except Exception as e:
            self.logger.error(f"Failed to extract Access data: {e}")
            raise
