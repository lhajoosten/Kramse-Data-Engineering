"""
Run the modular ETL pipeline
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def run_modular_pipeline():
    """Run the full modular ETL pipeline"""
    try:
        from src.pipeline import ETLPipeline
        
        print("=== Modular Kramse ETL Pipeline ===\n")
        
        # Initialize pipeline
        pipeline = ETLPipeline()
        
        # Test connections first
        print("Testing database connections...")
        if not pipeline.test_connections():
            print("❌ Database connection failed")
            return 1
        print("✅ Database connections successful\n")
        
        # Run full pipeline
        print("Starting full ETL pipeline...")
        results = pipeline.run_full_pipeline()
        
        # Display results
        print("\n=== Pipeline Results ===")
        for source_name, result in results.items():
            status = result.get('status', 'unknown')
            if status == 'success':
                if 'tables' in result:
                    # Access database with multiple tables
                    print(f"✅ {source_name}: Success")
                    for table_name, table_result in result['tables'].items():
                        table_status = table_result.get('status', 'unknown')
                        if table_status == 'success':
                            loaded = table_result.get('loaded', 0)
                            print(f"   - {table_name}: {loaded} records")
                        else:
                            error = table_result.get('error', 'Unknown error')
                            print(f"   - {table_name}: ❌ {error}")
                else:
                    # Single table source
                    loaded = result.get('loaded_records', 0)
                    table = result.get('table_name', 'unknown')
                    print(f"✅ {source_name}: {loaded} records loaded to {table}")
            else:
                error = result.get('error', result.get('reason', 'Unknown error'))
                print(f"❌ {source_name}: {error}")
        
        print("\n🎉 Modular ETL pipeline completed!")
        return 0
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(run_modular_pipeline())
