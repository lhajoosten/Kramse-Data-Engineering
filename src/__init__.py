"""
Kramse Data Engineering Package
Modern Python ETL voor containervervoer data
"""

# Modularized Python package imports
from .database import DatabaseManager
from .extractors import BaseExtractor, ContainerExtractor, ConsignorExtractor, EUMRVExtractor, AccessExtractor
from .transformers import BaseTransformer, ContainerTransformer, ConsignorTransformer, EUMRVTransformer
from .loaders import BaseLoader, BatchLoader, EUMRVLoader
from .pipeline import ETLPipeline

__version__ = "1.0.0"
__author__ = "Luc Joosten"
__all__ = [
    'DatabaseManager',
    'BaseExtractor', 'ContainerExtractor', 'ConsignorExtractor', 'EUMRVExtractor', 'AccessExtractor',
    'BaseTransformer', 'ContainerTransformer', 'ConsignorTransformer', 'EUMRVTransformer', 
    'BaseLoader', 'BatchLoader', 'EUMRVLoader',
    'ETLPipeline'
]
