"""
Container data extractor
"""
import os
import pandas as pd
from typing import Dict, Any
from .base import BaseExtractor

class ContainerExtractor(BaseExtractor):
    """Extractor for container data from text files"""
    
    def __init__(self, file_path: str = None):
        config = {
            'file_path': file_path or os.getenv('CONTAINER_FILE', 'data/Container v3.txt'),
            'delimiter': '\t',
            'encoding': 'latin-1'
        }
        super().__init__(config)
    
    def validate_source(self) -> bool:
        """Check if container file exists"""
        file_path = self.source_config['file_path']
        exists = os.path.exists(file_path)
        if not exists:
            self.logger.error(f"Container file not found: {file_path}")
        return exists
    
    def extract(self, source_path: str = None) -> pd.DataFrame:
        """Extract container data from text file"""
        try:
            # Use provided path or default from config
            file_path = source_path or self.source_config['file_path']
            
            if not self.validate_source():
                raise FileNotFoundError(f"Source file not accessible")
            
            self.logger.info(f"Extracting container data from: {file_path}")
            
            df = pd.read_csv(
                file_path,
                delimiter=self.source_config['delimiter'],
                encoding=self.source_config['encoding']
            )
            
            self.logger.info(f"Extracted {len(df)} container records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract container data: {e}")
            raise
