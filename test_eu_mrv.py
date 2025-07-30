#!/usr/bin/env python3
"""
Test script om alleen EU MRV data te laden
"""
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_engine_connection():
    """Create database engine using .env configuration"""
    try:
        server = os.getenv('DB_SERVER', 'localhost')
        database = os.getenv('DB_RAW', 'Kramse_RAW') 
        username = os.getenv('DB_USERNAME', 'sa')
        password = os.getenv('DB_PASSWORD')
        driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
        )
        
        engine = create_engine(connection_string)
        logger.info("Database connection established")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

def test_eu_mrv_only():
    """Test loading only EU MRV data"""
    try:
        engine = create_engine_connection()
        
        logger.info("Testing EU MRV data loading...")
        
        # Extract EU MRV data
        file_path = os.getenv('EU_MRV_FILE', 'data/2016-EU MRV Publication of information v5.csv')
        logger.info(f"Reading EU MRV data from {file_path}")
        
        df = pd.read_csv(file_path, encoding='latin-1')
        logger.info(f"Extracted {len(df)} EU MRV records")
        
        # Clean the data
        clean_df = df.copy()
        clean_df['source_file'] = '2016-EU MRV Publication of information v5.csv'
        clean_df['loaded_at'] = datetime.now()
        
        # Basic data cleaning
        for col in clean_df.columns:
            if clean_df[col].dtype == 'object':
                clean_df[col] = clean_df[col].astype(str).str.strip()
                clean_df[col] = clean_df[col].replace('nan', None)
        
        logger.info(f"Cleaned {len(clean_df)} EU MRV records")
        
        # Load to database in smaller batches
        batch_size = 100
        table_name = 'eu_mrv_shipping'
        
        logger.info(f"Loading {len(clean_df)} records to {table_name} in batches of {batch_size}")
        
        for i in range(0, len(clean_df), batch_size):
            batch_df = clean_df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(clean_df) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_df)} records)")
            
            # First batch replaces, subsequent batches append
            if_exists_batch = 'replace' if i == 0 else 'append'
            
            batch_df.to_sql(
                table_name,
                engine,
                if_exists=if_exists_batch,
                index=False,
                method='multi',
                chunksize=50
            )
            
            logger.info(f"Batch {batch_num} completed")
        
        logger.info("EU MRV test completed successfully!")
        
    except Exception as e:
        logger.error(f"EU MRV test failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    test_eu_mrv_only()
