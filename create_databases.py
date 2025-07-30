#!/usr/bin/env python3
"""
Script to create Kramse databases if they don't exist
"""
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def create_databases():
    """Create Kramse databases if they don't exist"""
    
    print("Reading environment variables...")
    # Connection to master database to create new databases
    server = os.getenv('DB_SERVER', 'localhost')
    username = os.getenv('DB_USERNAME', 'sa')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    print(f"Server: {server}")
    print(f"Username: {username}")
    print(f"Driver: {driver}")
    
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/master"
        f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
    )
    
    print("Creating engine...")
    engine = create_engine(connection_string)
    
    databases = ['Kramse_RAW', 'Kramse_STAGING', 'Kramse_DWH']
    
    try:
        print("Connecting to SQL Server...")
        with engine.connect() as conn:
            print("Connected successfully!")
            
            for db_name in databases:
                print(f"Processing database: {db_name}")
                
                # Check if database exists
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM sys.databases 
                    WHERE name = '{db_name}'
                """))
                
                count = result.scalar()
                print(f"Database {db_name} exists: {count > 0}")
                
                if count == 0:
                    # Create database
                    print(f"Creating database: {db_name}")
                    conn.execute(text(f"CREATE DATABASE [{db_name}]"))
                    conn.commit()
                    print(f"✅ Created database: {db_name}")
                else:
                    print(f"ℹ️  Database already exists: {db_name}")
                    
        print("\n🎉 All databases are ready!")
        
    except Exception as e:
        print(f"❌ Error creating databases: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    print("=== Creating Kramse Databases ===")
    create_databases()