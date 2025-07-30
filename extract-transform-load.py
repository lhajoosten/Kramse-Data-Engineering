"""
Simplified ETL Pipeline voor Kramse Data Engineering
Eenvoudige, werkende versie die alle data verwerkt.
"""
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.sql import func
import logging
import pyodbc
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KramseETL:
    """Simplified ETL for Kramse containervervoer data"""
    
    def __init__(self):
        """Initialize ETL with database connection"""
        self.engine = self._create_engine()
        
    def _create_engine(self):
        """Create database engine using .env configuration"""
        try:
            server = os.getenv('DB_SERVER', 'localhost')
            database = os.getenv('DB_RAW', 'Kramse_RAW') 
            username = os.getenv('DB_USERNAME', 'sa')
            password = os.getenv('DB_PASSWORD')
            driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
            trusted_connection = os.getenv('DB_TRUSTED_CONNECTION', 'no').lower()
            
            if trusted_connection == 'yes':
                # Windows Authentication
                connection_string = (
                    f"mssql+pyodbc://{server}/{database}"
                    f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes&Trusted_Connection=yes"
                )
            else:
                # SQL Server Authentication
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
    
    def extract_container_data(self, file_path=None):
        """Extract container data from text file"""
        try:
            if file_path is None:
                file_path = os.getenv('CONTAINER_FILE', 'data/Container v3.txt')
                
            logger.info(f"Reading container data from {file_path}")
            
            # Read the file with appropriate settings
            df = pd.read_csv(
                file_path,
                delimiter='\t',
                encoding='latin-1'
            )
            
            logger.info(f"Extracted {len(df)} container records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract container data: {e}")
            raise
    
    def extract_consignor_data(self, file_path=None):
        """Extract consignor data from CSV file"""
        try:
            if file_path is None:
                file_path = os.getenv('CONSIGNOR_FILE', 'data/Consignor.csv')
                
            logger.info(f"Reading consignor data from {file_path}")
            
            df = pd.read_csv(
                file_path,
                encoding='latin-1'
            )
            
            logger.info(f"Extracted {len(df)} consignor records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract consignor data: {e}")
            raise
    
    def extract_eu_mrv_data(self, file_path=None):
        """Extract EU MRV shipping data from CSV file with column optimization"""
        try:
            if file_path is None:
                file_path = os.getenv('EU_MRV_FILE', 'data/2016-EU MRV Publication of information v5.csv')
                
            logger.info(f"Reading EU MRV data from {file_path}")
            
            # First, let's see what's in the file
            df_sample = pd.read_csv(
                file_path,
                encoding='latin-1',
                nrows=5  # Just read first 5 rows to inspect
            )
            
            logger.info(f"EU MRV file has {len(df_sample.columns)} columns")
            logger.info(f"Sample columns: {list(df_sample.columns[:10])}")  # Show first 10
            
            # Now read the full dataset
            df_full = pd.read_csv(
                file_path,
                encoding='latin-1'
            )
            
            logger.info(f"Extracted {len(df_full)} EU MRV records with {len(df_full.columns)} columns")
            return df_full
            
        except Exception as e:
            logger.error(f"Failed to extract EU MRV data: {e}")
            raise
    
    def extract_access_data(self, file_path=None):
        """Extract data from MS Access database"""
        try:
            if file_path is None:
                file_path = os.getenv('ACCESSDB_FILE', 'data/KramseTPS v7.mdb')
                
            logger.info(f"Reading Access database from {file_path}")
            
            # Check if file exists
            if not Path(file_path).exists():
                logger.error(f"Access database file not found: {file_path}")
                return {}
            
            # Connection string for MS Access
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                f'DBQ={Path(file_path).absolute()};'
            )
            
            # Connect to Access database
            conn = pyodbc.connect(conn_str)
            
            # Get list of tables
            cursor = conn.cursor()
            tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
            logger.info(f"Found {len(tables)} tables in Access database: {tables}")
            
            # Extract all tables
            access_data = {}
            for table_name in tables:
                try:
                    query = f"SELECT * FROM [{table_name}]"
                    df = pd.read_sql(query, conn)
                    
                    # Basic data cleaning like other methods
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].astype(str).str.strip()
                            df[col] = df[col].replace('nan', None)
                    
                    access_data[table_name] = df
                    logger.info(f"Extracted {len(df)} records from table: {table_name}")
                except Exception as e:
                    logger.warning(f"Could not extract table {table_name}: {e}")
            
            conn.close()
            return access_data
            
        except Exception as e:
            logger.error(f"Failed to extract Access data: {e}")
            raise
    
    def _clean_column_name(self, col_name):
        """Clean column names for SQL Server compatibility"""
        import re
        
        # Convert to string and replace problematic characters
        clean_name = str(col_name)
        
        # Replace spaces and special characters with underscores
        clean_name = re.sub(r'[^\w]', '_', clean_name)
        
        # Remove multiple underscores
        clean_name = re.sub(r'_+', '_', clean_name)
        
        # Remove leading/trailing underscores
        clean_name = clean_name.strip('_')
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = 'col_' + clean_name
        
        # Limit length to 100 characters (SQL Server friendly)
        clean_name = clean_name[:100]
        
        return clean_name if clean_name else 'unnamed_column'
    
    def clean_container_data(self, df):
        """Clean and transform container data"""
        try:
            logger.info("Cleaning container data")
            
            # Create a copy to avoid modifying original
            clean_df = df.copy()
            
            # Add metadata columns
            clean_df['source_file'] = 'Container v3.txt'
            clean_df['loaded_at'] = datetime.now()
            
            # Basic data cleaning
            for col in clean_df.columns:
                if clean_df[col].dtype == 'object':
                    clean_df[col] = clean_df[col].astype(str).str.strip()
                    clean_df[col] = clean_df[col].replace('nan', None)
            
            logger.info(f"Cleaned {len(clean_df)} container records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Failed to clean container data: {e}")
            raise
    
    def clean_consignor_data(self, df):
        """Clean and transform consignor data"""
        try:
            logger.info("Cleaning consignor data")
            
            clean_df = df.copy()
            
            # Add metadata columns
            clean_df['source_file'] = 'Consignor.csv'
            clean_df['loaded_at'] = datetime.now()
            
            # Basic data cleaning
            for col in clean_df.columns:
                if clean_df[col].dtype == 'object':
                    clean_df[col] = clean_df[col].astype(str).str.strip()
                    clean_df[col] = clean_df[col].replace('nan', None)
            
            logger.info(f"Cleaned {len(clean_df)} consignor records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Failed to clean consignor data: {e}")
            raise
    
    def clean_eu_mrv_data(self, df):
        """Clean and prepare EU MRV data for loading - using ALL columns"""
        try:
            logger.info(f"Starting cleaning of EU MRV data with {len(df)} records and {len(df.columns)} columns")
            
            # Keep ALL columns instead of limiting them
            logger.info(f"Processing all {len(df.columns)} columns from EU MRV data")
            
            # Clean column names for SQL Server compatibility
            original_columns = df.columns.tolist()
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            logger.info(f"Sample column mapping (first 10): {dict(list(zip(original_columns[:10], df.columns[:10])))}...")
            
            # Basic data cleaning
            df = df.fillna('')  # Replace NaN with empty strings
            
            # Add metadata columns
            df['processed_date'] = pd.Timestamp.now()
            df['source_file'] = 'eu_mrv_shipping'
            df['total_columns_in_source'] = len(original_columns)
            
            logger.info(f"EU MRV data cleaned successfully: {len(df)} records, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Failed to clean EU MRV data: {e}")
            raise
    
    def load_eu_mrv_to_database(self, df, target_db="Kramse_RAW"):
        """Special loading method for EU MRV data - record by record for all columns"""
        try:
            table_name = "eu_mrv_shipping"
            
            logger.info(f"Loading {len(df)} EU MRV records with {len(df.columns)} columns to {target_db}.{table_name} one by one")
            
            # Create engine for specific target database
            server = os.getenv('DB_SERVER', 'localhost')
            username = os.getenv('DB_USERNAME', 'sa')
            password = os.getenv('DB_PASSWORD')
            driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
            
            connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{target_db}?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
            engine = sa.create_engine(connection_string)
            
            # Load record by record to avoid parameter limits
            total_loaded = 0
            failed_records = 0
            
            # First record replaces table, then append
            for i, (index, row) in enumerate(df.iterrows()):
                try:
                    row_df = pd.DataFrame([row])
                    row_df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='replace' if i == 0 else 'append',
                        index=False,
                        method=None  # Use default method for single records
                    )
                    total_loaded += 1
                    
                    if (i + 1) % 100 == 0:  # Log every 100 records
                        logger.info(f"Loaded {i + 1} records so far...")
                        
                except Exception as record_error:
                    failed_records += 1
                    logger.warning(f"Failed to load record {i + 1}: {record_error}")
                    continue
            
            logger.info(f"EU MRV data loading completed: {total_loaded}/{len(df)} records loaded successfully ({failed_records} failed)")
            return total_loaded
            
        except Exception as e:
            logger.error(f"Failed to load EU MRV data: {e}")
            return 0
    
    def clean_access_data(self, access_data_dict):
        """Clean and transform MS Access data"""
        try:
            logger.info("Cleaning Access database data")
            
            cleaned_data = {}
            for table_name, df in access_data_dict.items():
                logger.info(f"Cleaning Access table: {table_name}")
                
                clean_df = df.copy()
                
                # Add metadata columns
                clean_df['source_table'] = table_name
                clean_df['source_database'] = 'KramseTPS v7.mdb'
                clean_df['loaded_at'] = datetime.now()
                
                # Basic data cleaning
                for col in clean_df.columns:
                    if clean_df[col].dtype == 'object':
                        clean_df[col] = clean_df[col].astype(str).str.strip()
                        clean_df[col] = clean_df[col].replace('nan', None)
                
                cleaned_data[table_name] = clean_df
                logger.info(f"Cleaned {len(clean_df)} records from {table_name}")
            
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Failed to clean Access data: {e}")
            raise
    
    def load_to_database(self, df, table_name, if_exists='replace', batch_size=500):
        """Load dataframe to database table in batches"""
        try:
            logger.info(f"Loading {len(df)} records to {table_name}")
            
            # For large datasets, use batches
            if len(df) > batch_size:
                logger.info(f"Using batch processing with batch size: {batch_size}")
                
                # Split dataframe into chunks
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size]
                    batch_num = (i // batch_size) + 1
                    total_batches = (len(df) + batch_size - 1) // batch_size
                    
                    logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_df)} records)")
                    
                    # First batch replaces, subsequent batches append
                    if_exists_batch = 'replace' if i == 0 else 'append'
                    
                    batch_df.to_sql(
                        table_name,
                        self.engine,
                        if_exists=if_exists_batch,
                        index=False,
                        method='multi',
                        chunksize=250
                    )
                    
                    logger.info(f"Batch {batch_num} completed")
            else:
                # Small datasets - load normally
                df.to_sql(
                    table_name,
                    self.engine,
                    if_exists=if_exists,
                    index=False,
                    method='multi',
                    chunksize=250
                )
            
            logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {e}")
            raise
    
    def run_full_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("Starting Kramse ETL Pipeline")
            
            # Extract, clean and load container data
            logger.info("Processing container data...")
            container_df = self.extract_container_data()
            clean_container_df = self.clean_container_data(container_df)
            self.load_to_database(clean_container_df, 'containers')
            
            # Extract, clean and load consignor data
            logger.info("Processing consignor data...")
            consignor_df = self.extract_consignor_data()
            clean_consignor_df = self.clean_consignor_data(consignor_df)
            self.load_to_database(clean_consignor_df, 'consignors')
            
            # Extract, clean and load EU MRV data with special handling
            logger.info("Processing EU MRV data with column limit handling...")
            try:
                eu_mrv_df = self.extract_eu_mrv_data()
                if eu_mrv_df is not None and not eu_mrv_df.empty:
                    clean_eu_mrv_df = self.clean_eu_mrv_data(eu_mrv_df)
                    loaded_count = self.load_eu_mrv_to_database(clean_eu_mrv_df)
                    logger.info(f"EU MRV processing completed: {loaded_count} records loaded")
                else:
                    logger.warning("No EU MRV data found to process")
            except Exception as eu_error:
                logger.error(f"EU MRV processing failed: {eu_error}")
                logger.info("Continuing with rest of pipeline...")
            
            # Extract, clean and load Access database data
            logger.info("Processing MS Access database...")
            access_data = self.extract_access_data()
            if access_data:  # Only process if we got data
                cleaned_access_data = self.clean_access_data(access_data)
                
                # Load each Access table as separate database table
                for table_name, df in cleaned_access_data.items():
                    # Create table name with prefix to avoid conflicts
                    db_table_name = f"access_{table_name.lower()}"
                    self.load_to_database(df, db_table_name)
            else:
                logger.warning("No Access data found to process")
            
            logger.info("ETL Pipeline completed successfully!")
            
            # Show summary
            self.show_summary()
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise
    
    def show_summary(self):
        """Show summary of loaded data"""
        try:
            logger.info("=== ETL SUMMARY ===")
            
            total_records = 0
            
            with self.engine.connect() as conn:
                # Container summary
                result = conn.execute(sa.text("SELECT COUNT(*) as count FROM containers"))
                container_count = result.fetchone()[0]
                logger.info(f"Containers loaded: {container_count}")
                total_records += container_count
                
                # Consignor summary
                result = conn.execute(sa.text("SELECT COUNT(*) as count FROM consignors"))
                consignor_count = result.fetchone()[0]
                logger.info(f"Consignors loaded: {consignor_count}")
                total_records += consignor_count
                
                # EU MRV summary
                try:
                    result = conn.execute(sa.text("SELECT COUNT(*) as count FROM eu_mrv_shipping"))
                    eu_mrv_count = result.fetchone()[0]
                    logger.info(f"EU MRV records loaded: {eu_mrv_count}")
                    total_records += eu_mrv_count
                except Exception as eu_error:
                    logger.warning(f"Could not get EU MRV count (table may not exist): {eu_error}")
                    eu_mrv_count = 0
                
                # Access tables summary
                try:
                    # Get all tables that start with 'access_'
                    tables_result = conn.execute(sa.text("""
                        SELECT TABLE_NAME 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_NAME LIKE 'access_%'
                    """))
                    access_tables = [row[0] for row in tables_result]
                    
                    if access_tables:
                        logger.info("Access database tables:")
                        for table_name in access_tables:
                            result = conn.execute(sa.text(f"SELECT COUNT(*) as count FROM {table_name}"))
                            table_count = result.fetchone()[0]
                            logger.info(f"  {table_name}: {table_count} records")
                            total_records += table_count
                    else:
                        logger.info("No Access database tables found")
                        
                except Exception as access_error:
                    logger.warning(f"Could not get Access table summary: {access_error}")
                
                logger.info(f"Total records across all tables: {total_records}")
                
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")

def main():
    """Main function to run the ETL pipeline"""
    try:
        etl = KramseETL()
        etl.run_full_pipeline()
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
