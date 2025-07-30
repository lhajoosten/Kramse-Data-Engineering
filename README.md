# Kramse Data Engineering

Een modern Python ETL project voor containervervoer data analyse. Vervangt het originele SSIS-based systeem met een modulaire Python architectuur.

## 🚀 Quick Start

### 1. Setup
```powershell
# Clone repository
git clone <your-repo-url>
cd Kramse-Data-Engineering

# Install dependencies
pip install -r requirements.txt

# Copy environment template
copy .env.template .env
# Edit .env met jouw SQL Server details
```

### 2. Database Setup
```powershell
# Check MS Access drivers (optioneel)
python check_access_driver.py

# Create databases
python create_databases.py
```

### 3. Run Pipeline
```powershell
# Test modulaire architectuur
python test_modular.py

# Run volledige ETL pipeline
python run_modular.py

# Of via command line
python main.py --source all
```

## 📊 Data Sources

Het project verwerkt 4 data bronnen:
- **Container v3.txt** (8 records) - Container eigenschappen
- **Consignor.csv** (51 records) - Consignor informatie  
- **EU MRV CSV** (10,940 records) - EU shipping emissions
- **MS Access DB** (310 records, 7 tabellen) - Kramse TPS data

**Totaal: 11,331 records**

## 🏗️ Modulaire Architectuur

```
src/
├── database/       # DatabaseManager voor connecties
├── extractors/     # Data extraction per bron
├── transformers/   # Data cleaning & business rules
├── loaders/        # Database loading strategieën
└── pipeline/       # ETL orchestration
```

## 🎯 Gebruik

### Eenvoudig (aanbevolen)
```powershell
python run_modular.py    # Volledige pipeline
python test_modular.py   # Test alle components
```

### Command Line
```powershell
python main.py --source all         # Alle bronnen
python main.py --source container   # Alleen containers
python main.py --test-connection    # Test database
```

### Programmatisch
```python
from src.pipeline import ETLPipeline

pipeline = ETLPipeline()
results = pipeline.run_full_pipeline()
```

## 🔧 Setup Scripts

- **`create_databases.py`** - Maak Kramse databases aan
- **`check_access_driver.py`** - Test MS Access ODBC drivers

## 📋 Requirements

- Python 3.8+
- SQL Server (met ODBC Driver 17)
- MS Access Driver (voor .mdb files)

## 🗄️ Database Layers

- **Kramse_RAW** - Ruwe data zoals aangeleverd
- **Kramse_STAGING** - Gecleande data (toekomstig)
- **Kramse_DWH** - Data warehouse (toekomstig)

## ✅ Resultaten

De modulaire pipeline verwerkt succesvol:
- ✅ Container data: 8 records
- ✅ Consignor data: 51 records  
- ✅ EU MRV data: 10,940 records (alle 59 kolommen)
- ✅ MS Access data: 310 records uit 7 tabellen

**Totaal: 11,331 records in SQL Server**