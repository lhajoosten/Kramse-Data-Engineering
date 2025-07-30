"""
Consignor data extractor
"""
import os
import pandas as pd
from .base import BaseExtractor

class ConsignorExtractor(BaseExtractor):
    """Extractor for consignor data from CSV files"""
    
    def __init__(self, file_path: str = None):
        config = {
            'file_path': file_path or os.getenv('CONSIGNOR_FILE', 'data/Consignor.csv'),
            'encoding': 'latin-1'
        }
        super().__init__(config)
    
    def validate_source(self) -> bool:
        """Check if consignor file exists"""
        file_path = self.source_config['file_path']
        exists = os.path.exists(file_path)
        if not exists:
            self.logger.error(f"Consignor file not found: {file_path}")
        return exists
    
    def extract(self, source_path: str = None) -> pd.DataFrame:
        """Extract consignor data from CSV file"""
        try:
            # Use provided path or default from config
            file_path = source_path or self.source_config['file_path']
            
            if not self.validate_source():
                raise FileNotFoundError(f"Source file not accessible")
            
            self.logger.info(f"Extracting consignor data from: {file_path}")
            
            df = pd.read_csv(
                file_path,
                encoding=self.source_config['encoding']
            )
            
            self.logger.info(f"Extracted {len(df)} consignor records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract consignor data: {e}")
            raise
