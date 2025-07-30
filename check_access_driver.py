#!/usr/bin/env python3
"""
Script to check if MS Access ODBC drivers are available
"""
import pyodbc

def check_access_drivers():
    """Check for available MS Access drivers"""
    print("Checking for MS Access ODBC drivers...")
    
    # List all available drivers
    drivers = pyodbc.drivers()
    print(f"\nAll available ODBC drivers ({len(drivers)}):")
    for i, driver in enumerate(drivers, 1):
        print(f"{i:2}. {driver}")
    
    # Check for Access-specific drivers
    access_drivers = [d for d in drivers if 'access' in d.lower() or 'microsoft office' in d.lower()]
    
    print(f"\nMS Access compatible drivers ({len(access_drivers)}):")
    if access_drivers:
        for i, driver in enumerate(access_drivers, 1):
            print(f"{i}. {driver}")
    else:
        print("❌ No MS Access drivers found!")
        print("\nYou may need to install:")
        print("- Microsoft Access Database Engine 2016 Redistributable")
        print("- Or Microsoft Office/Access")
        return False
    
    return True

def test_access_connection():
    """Test connection to the Access database"""
    from pathlib import Path
    
    access_file = "data/KramseTPS v7.mdb"
    
    if not Path(access_file).exists():
        print(f"\n❌ Access database file not found: {access_file}")
        return False
    
    print(f"\n✅ Access database file found: {access_file}")
    
    # Try to connect
    try:
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            f'DBQ={Path(access_file).absolute()};'
        )
        
        print("Testing connection...")
        conn = pyodbc.connect(conn_str)
        
        # Get tables
        cursor = conn.cursor()
        tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
        
        print(f"✅ Connection successful!")
        print(f"Found {len(tables)} tables:")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("=== MS Access Driver Check ===")
    
    drivers_ok = check_access_drivers()
    
    if drivers_ok:
        test_access_connection()
    
    print("\n=== Check Complete ===")
