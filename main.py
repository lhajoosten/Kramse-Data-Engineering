"""
Main entry point for modular ETL pipeline
"""
import sys
import argparse
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

try:
    from pipeline import ETLPipeline
except ImportError:
    # Fallback import method
    import importlib.util
    spec = importlib.util.spec_from_file_location("pipeline", Path(__file__).parent / "src" / "pipeline" / "__init__.py")
    pipeline_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pipeline_module)
    ETLPipeline = pipeline_module.ETLPipeline

def main():
    """Main function to run ETL pipeline"""
    parser = argparse.ArgumentParser(description='Kramse Data Engineering ETL Pipeline')
    parser.add_argument('--source', choices=['all', 'container', 'consignor', 'eu_mrv', 'access'], 
                       default='all', help='Data source to process')
    parser.add_argument('--config', default='config/database.yaml', 
                       help='Path to configuration file')
    parser.add_argument('--test-connection', action='store_true', 
                       help='Test database connection only')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    try:
        pipeline = ETLPipeline(args.config)
        
        # Test connection if requested
        if args.test_connection:
            if pipeline.test_connections():
                print("✅ Database connections successful")
                return 0
            else:
                print("❌ Database connection failed")
                return 1
        
        # Run pipeline
        if args.source == 'all':
            results = pipeline.run_full_pipeline()
            print("\n=== Full Pipeline Results ===")
            for source, result in results.items():
                status = result.get('status', 'unknown')
                if status == 'success':
                    loaded = result.get('loaded_records', result.get('tables', 'N/A'))
                    print(f"✅ {source}: {loaded} records loaded")
                else:
                    error = result.get('error', result.get('reason', 'Unknown error'))
                    print(f"❌ {source}: {error}")
        else:
            result = pipeline.run_single_source(args.source)
            status = result.get('status', 'unknown')
            if status == 'success':
                loaded = result.get('loaded_records', result.get('tables', 'N/A'))
                print(f"✅ {args.source}: {loaded} records loaded")
            else:
                error = result.get('error', result.get('reason', 'Unknown error'))
                print(f"❌ {args.source}: {error}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
