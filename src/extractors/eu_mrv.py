"""
EU MRV data extractor
"""
import os
import pandas as pd
from .base import BaseExtractor

class EUMRVExtractor(BaseExtractor):
    """Extractor for EU MRV shipping data from CSV files"""
    
    def __init__(self, file_path: str = None):
        config = {
            'file_path': file_path or os.getenv('EU_MRV_FILE', 'data/2016-EU MRV Publication of information v5.csv'),
            'encoding': 'latin-1'
        }
        super().__init__(config)
    
    def validate_source(self) -> bool:
        """Check if EU MRV file exists"""
        file_path = self.source_config['file_path']
        exists = os.path.exists(file_path)
        if not exists:
            self.logger.error(f"EU MRV file not found: {file_path}")
        return exists
    
    def extract(self, source_path: str = None) -> pd.DataFrame:
        """Extract EU MRV data from CSV file"""
        try:
            # Use provided path or default from config
            file_path = source_path or self.source_config['file_path']
            
            if not self.validate_source():
                raise FileNotFoundError(f"Source file not accessible")
            
            self.logger.info(f"Extracting EU MRV data from: {file_path}")
            
            # Sample first to check structure
            df_sample = pd.read_csv(
                file_path,
                encoding=self.source_config['encoding'],
                nrows=5
            )
            
            self.logger.info(f"EU MRV file has {len(df_sample.columns)} columns")
            self.logger.info(f"Sample columns: {list(df_sample.columns[:10])}")
            
            # Read full dataset
            df = pd.read_csv(
                file_path,
                encoding=self.source_config['encoding']
            )
            
            self.logger.info(f"Extracted {len(df)} EU MRV records with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract EU MRV data: {e}")
            raise
