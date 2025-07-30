"""
Base extractor class for all data sources
"""
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Abstract base class for all data extractors"""
    
    def __init__(self, source_config: Dict[str, Any]):
        self.source_config = source_config
        self.logger = logger
    
    @abstractmethod
    def extract(self, source_path: str) -> pd.DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def validate_source(self) -> bool:
        """Validate that data source is accessible"""
        pass
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get metadata about the data source"""
        return {
            'extractor_type': self.__class__.__name__,
            'source_config': self.source_config
        }
