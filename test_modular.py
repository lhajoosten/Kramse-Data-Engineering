"""
Simple test script for the modular ETL pipeline
"""
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test if all imports work correctly"""
    try:
        print("Testing database manager...")
        from src.database import DatabaseManager
        print("✅ DatabaseManager import successful")
        
        print("Testing extractors...")
        from src.extractors import BaseExtractor, ContainerExtractor, ConsignorExtractor, EUMRVExtractor, AccessExtractor
        print("✅ Extractors import successful")
        
        print("Testing transformers...")  
        from src.transformers import BaseTransformer, ContainerTransformer, ConsignorTransformer, EUMRVTransformer
        print("✅ Transformers import successful")
        
        print("Testing loaders...")
        from src.loaders import BaseLoader, BatchLoader, EUMRVLoader
        print("✅ Loaders import successful")
        
        print("Testing pipeline...")
        from src.pipeline import ETLPipeline
        print("✅ ETLPipeline import successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_database_connection():
    """Test database connection"""
    try:
        from src.database import DatabaseManager
        db_manager = DatabaseManager()
        
        if db_manager.test_connection('Kramse_RAW'):
            print("✅ Database connection successful")
            return True
        else:
            print("❌ Database connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_single_extractor():
    """Test a single extractor"""
    try:
        from src.extractors import ContainerExtractor
        
        extractor = ContainerExtractor()
        if extractor.validate_source():
            data = extractor.extract()
            print(f"✅ Container extractor successful: {len(data)} records")
            return True
        else:
            print("❌ Container source file not found")
            return False
            
    except Exception as e:
        print(f"❌ Extractor test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Modular ETL Pipeline Tests ===\n")
    
    success = True
    
    print("1. Testing imports...")
    success &= test_imports()
    print()
    
    print("2. Testing database connection...")
    success &= test_database_connection()
    print()
    
    print("3. Testing single extractor...")
    success &= test_single_extractor()
    print()
    
    if success:
        print("🎉 All tests passed! De modulaire architectuur is gereed.")
    else:
        print("❌ Some tests failed. Check the errors above.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
