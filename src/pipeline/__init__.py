"""
Main ETL pipeline orchestrator that coordinates all components
"""
import os
import logging
from typing import Dict, List, Any
from pathlib import Path

from ..database import DatabaseManager
from ..extractors import ContainerExtractor, ConsignorExtractor, EUMRVExtractor, AccessExtractor
from ..transformers import ContainerTransformer, ConsignorTransformer, EUMRVTransformer
from ..loaders import BatchLoader, EUMRVLoader

class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config/database.yaml"):
        """Initialize pipeline with configuration"""
        self.config_path = config_path
        self.db_manager = DatabaseManager(config_path)
        self.logger = self._setup_logging()
        
        # Initialize components
        self.extractors = {
            'container': ContainerExtractor(),
            'consignor': ConsignorExtractor(), 
            'eu_mrv': EUMRVExtractor(),
            'access': AccessExtractor()
        }
        
        self.transformers = {
            'container': ContainerTransformer(),
            'consignor': ConsignorTransformer(),
            'eu_mrv': EUMRVTransformer()
        }
        
        self.loaders = {
            'batch': BatchLoader(config_path),
            'eu_mrv': EUMRVLoader(config_path)
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for pipeline"""
        logger = logging.getLogger('etl_pipeline')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # File handler
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            file_handler = logging.FileHandler(log_dir / "pipeline.log")
            file_handler.setLevel(logging.DEBUG)
            
            # Formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        
        return logger
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run complete ETL pipeline for all data sources"""
        self.logger.info("Starting full ETL pipeline")
        results = {}
        
        # Data source configurations
        data_sources = {
            'container': {
                'source_path': 'data/Container v3.txt',
                'table_name': 'raw_container',
                'loader': 'batch'
            },
            'consignor': {
                'source_path': 'data/Consignor.csv',
                'table_name': 'raw_consignor',
                'loader': 'batch'
            },
            'eu_mrv': {
                'source_path': 'data/2016-EU MRV Publication of information v5.csv',
                'table_name': 'raw_eu_mrv',
                'loader': 'eu_mrv'
            }
        }
        
        # Process each data source
        for source_name, config in data_sources.items():
            try:
                result = self.process_data_source(
                    source_name=source_name,
                    source_path=config['source_path'],
                    table_name=config['table_name'],
                    loader_type=config['loader']
                )
                results[source_name] = result
                
            except Exception as e:
                self.logger.error(f"Failed to process {source_name}: {e}")
                results[source_name] = {'status': 'failed', 'error': str(e)}
        
        # Process Access database separately
        try:
            access_result = self.process_access_database()
            results['access'] = access_result
        except Exception as e:
            self.logger.error(f"Failed to process Access database: {e}")
            results['access'] = {'status': 'failed', 'error': str(e)}
        
        self.logger.info("Full ETL pipeline completed")
        return results
    
    def process_data_source(self, source_name: str, source_path: str, table_name: str, loader_type: str) -> Dict[str, Any]:
        """Process a single data source through extract-transform-load"""
        self.logger.info(f"Processing {source_name} from {source_path}")
        
        try:
            # Extract
            extractor = self.extractors[source_name]
            raw_data = extractor.extract(source_path)
            
            if raw_data is None or raw_data.empty:
                return {'status': 'failed', 'reason': 'No data extracted'}
            
            self.logger.info(f"Extracted {len(raw_data)} records from {source_name}")
            
            # Transform
            if source_name in self.transformers:
                transformer = self.transformers[source_name]
                transformed_data = transformer.transform(raw_data)
                self.logger.info(f"Transformed {source_name} data")
            else:
                transformed_data = raw_data
                self.logger.info(f"No specific transformer for {source_name}, using raw data")
            
            # Load
            loader = self.loaders[loader_type]
            loaded_count = loader.load(transformed_data, table_name)
            
            return {
                'status': 'success',
                'extracted_records': len(raw_data),
                'loaded_records': loaded_count,
                'table_name': table_name
            }
            
        except Exception as e:
            self.logger.error(f"Error processing {source_name}: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def process_access_database(self) -> Dict[str, Any]:
        """Process Access database with multiple tables"""
        self.logger.info("Processing Access database")
        
        try:
            extractor = self.extractors['access']
            access_data = extractor.extract('data/KramseTPS v7.mdb')
            
            if not access_data:
                return {'status': 'failed', 'reason': 'No Access data extracted'}
            
            results = {}
            loader = self.loaders['batch']
            
            for table_name, df in access_data.items():
                try:
                    loaded_count = loader.load(df, f"raw_access_{table_name}")
                    results[table_name] = {
                        'status': 'success',
                        'records': len(df),
                        'loaded': loaded_count
                    }
                    self.logger.info(f"Loaded {loaded_count} records from Access table {table_name}")
                    
                except Exception as table_error:
                    results[table_name] = {'status': 'failed', 'error': str(table_error)}
                    self.logger.error(f"Failed to load Access table {table_name}: {table_error}")
            
            return {'status': 'success', 'tables': results}
            
        except Exception as e:
            self.logger.error(f"Error processing Access database: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def test_connections(self) -> bool:
        """Test all database connections"""
        return self.db_manager.test_connection('Kramse_RAW')
    
    def run_single_source(self, source_name: str) -> Dict[str, Any]:
        """Run ETL for a single data source"""
        if source_name == 'access':
            return self.process_access_database()
        
        # Map source names to configurations
        source_configs = {
            'container': ('data/Container v3.txt', 'raw_container', 'batch'),
            'consignor': ('data/Consignor.csv', 'raw_consignor', 'batch'),
            'eu_mrv': ('data/2016-EU MRV Publication of information v5.csv', 'raw_eu_mrv', 'eu_mrv')
        }
        
        if source_name not in source_configs:
            return {'status': 'failed', 'error': f'Unknown source: {source_name}'}
        
        source_path, table_name, loader_type = source_configs[source_name]
        return self.process_data_source(source_name, source_path, table_name, loader_type)
