"""
Data models and table definitions
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class ContainerData(Base):
    """Container data model"""
    __tablename__ = 'containers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Add container-specific columns based on your data
    source_file = Column(String(255))
    loaded_at = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

class ConsignorData(Base):
    """Consignor data model"""
    __tablename__ = 'consignors'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Add consignor-specific columns based on your data
    source_file = Column(String(255))
    loaded_at = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

class EUMRVData(Base):
    """EU MRV shipping data model"""
    __tablename__ = 'eu_mrv_shipping'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Dynamic columns will be created based on CSV structure
    source_file = Column(String(255))
    total_columns_in_source = Column(Integer)
    processed_date = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

class LoadMetadata(Base):
    """ETL metadata tracking"""
    __tablename__ = 'etl_metadata'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(255), nullable=False)
    source_file = Column(String(500))
    records_processed = Column(Integer)
    records_loaded = Column(Integer)
    status = Column(String(50))  # SUCCESS, FAILED, PARTIAL
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    error_message = Column(Text)
    created_at = Column(DateTime, default=func.now())
