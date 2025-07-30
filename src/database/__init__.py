"""
Database connection and engine management
"""
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from typing import Optional
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Centralized database connection management"""
    
    def __init__(self, config_path: str = None):
        self._engines = {}
        self._connection_config = self._load_config()
    
    def _load_config(self) -> dict:
        """Load database configuration from environment"""
        return {
            'server': os.getenv('DB_SERVER', 'localhost'),
            'username': os.getenv('DB_USERNAME', 'sa'),
            'password': os.getenv('DB_PASSWORD'),
            'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'),
            'trusted_connection': os.getenv('DB_TRUSTED_CONNECTION', 'no').lower() == 'yes'
        }
    
    def get_engine(self, database: str) -> sa.Engine:
        """Get or create database engine for specific database"""
        if database not in self._engines:
            self._engines[database] = self._create_engine(database)
        return self._engines[database]
    
    def _create_engine(self, database: str) -> sa.Engine:
        """Create SQLAlchemy engine for database"""
        try:
            config = self._connection_config
            
            if config['trusted_connection']:
                connection_string = (
                    f"mssql+pyodbc://{config['server']}/{database}"
                    f"?driver={config['driver'].replace(' ', '+')}&TrustServerCertificate=yes&Trusted_Connection=yes"
                )
            else:
                connection_string = (
                    f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{database}"
                    f"?driver={config['driver'].replace(' ', '+')}&TrustServerCertificate=yes"
                )
            
            engine = create_engine(connection_string)
            logger.info(f"Database engine created for: {database}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create engine for {database}: {e}")
            raise
    
    def test_connection(self, database: str) -> bool:
        """Test database connection"""
        try:
            engine = self.get_engine(database)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connection test successful for: {database}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed for {database}: {e}")
            return False
    
    def close_all(self):
        """Close all database connections"""
        for db_name, engine in self._engines.items():
            engine.dispose()
            logger.info(f"Closed connection to: {db_name}")
        self._engines.clear()

# Global database manager instance
db_manager = DatabaseManager()
