# Kramse Data Engineering

Een modern Python data engineering project gebaseerd op het originele Kramse containervervoer Business Intelligence project. Dit project bouwt een volledig Python-based ETL pipeline ter vervanging van de oorspronkelijke SSIS implementatie.

## 🚀 Project Overview

Het Kramse project analyseert containervervoer data met focus op:
- Container eigenschappen en pricing modellen
- Consignor informatie en korting structuren  
- EU MRV (Monitoring, Reporting and Verification) shipping data
- Business intelligence en sustainability analytics

## 🛠️ Technologie Stack

- **Python 3.8+** - Core programming language
- **SQLAlchemy** - Database ORM en migrations
- **Pandas** - Data processing en analysis
- **SQL Server** - Data warehouse platform
- **Great Expectations** - Data quality en validatie
- **Loguru** - Advanced logging
- **Jupyter** - Data exploration notebooks
- **Apache Airflow/Prefect** - Workflow orchestration

## 📊 Data Sources

Het project verwerkt de volgende data bronnen:
- `Container v3.txt` - Container eigenschappen en pricing data
- `Consignor.csv` - Consignor master data met discount informatie
- `2016-EU MRV Publication of information v5.csv` - EU shipping emissions data

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RAW LAYER     │    │  STAGING LAYER  │    │ PRODUCTION DWH  │
│                 │    │                 │    │                 │
│ • Raw Container │--->│ • Cleaned Data  │--->│ • Fact Tables   │
│ • Raw Consignor │    │ • Validated     │    │ • Dim Tables    │
│ • Raw EU MRV    │    │ • Transformed   │    │ • Analytics     │
│ • Load Metadata │    │ • Business Rules│    │ • KPIs & Metrics│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## ⚡ Quick Start

### 1. Clone en Setup
```powershell
git clone https://github.com/lhajoosten/Kramse-Data-Engineering.git
cd Kramse-Data-Engineering

# Maak virtual environment
python -m venv venv
venv\Scripts\activate

# Installeer project
python setup_project.py
```

### 2. Configureer Database
```powershell
# Edit .env file met jouw SQL Server details
copy .env.template .env
notepad .env

# Setup database schemas
sqlcmd -S localhost -i sql\create_schemas.sql
sqlcmd -S localhost -d Kramse_RAW -i sql\raw_tables.sql
```

### 3. Test de Pipeline
```powershell
# Test data extraction
python main.py --stage extract --source container

# Run complete ETL
python main.py --stage full --source all

# Explore data
jupyter notebook notebooks\data_exploration.ipynb
```

## 📁 Project Structure

```
kramse-data-engineering/
├── src/
│   ├── extract/          # Data extraction (CSV, TXT readers)
│   ├── transform/        # Data cleaning & business rules
│   ├── load/            # Database loading utilities
│   ├── models/          # SQLAlchemy data models
│   ├── pipelines/       # ETL orchestration
│   └── utils/           # Database, logging, config
├── config/              # YAML configuration files
├── Data/                # Source data files
│   ├── Container v3.txt
│   ├── Consignor.csv
│   └── 2016-EU MRV Publication of information v5.csv
├── sql/                 # Database DDL scripts
├── tests/               # Unit & integration tests
├── notebooks/           # Jupyter data exploration
├── logs/                # Application logs
└── docs/                # Documentation
```

## 🔧 Development

**Installeer development dependencies**
```powershell
pip install -e ".[dev]"
```

**Run tests**
```powershell
pytest tests/ -v --cov=src
```

**Code quality**
```powershell
black src/
flake8 src/
mypy src/
```

## 🗄️ Database Layers

### RAW Layer (`Kramse_RAW`)
- Originele data zoals aangeleverd
- Minimale transformaties
- Data lineage tracking

### STAGING Layer (`Kramse_STAGING`)
- Gecleande en gevalideerde data
- Business rules toegepast
- Data quality checks

### PRODUCTION Layer (`Kramse_DWH`)
- Star schema data warehouse
- Optimized voor analytics
- Aggregated metrics en KPIs

## 📈 Pipeline Features

- **Incremental Loading** - Delta processing
- **Data Quality Checks** - Great Expectations validatie
- **Error Handling** - Retry logic en fallback scenarios
- **Monitoring** - Comprehensive logging en metrics
- **Scalability** - Batch processing met configureerbare batch sizes

## 🚀 Usage Examples

### Extract specifieke data source
```python
from src.pipelines.etl_pipeline import ETLPipeline

pipeline = ETLPipeline()
container_data = pipeline.extract_container_data()
print(f"Extracted {len(container_data)} container records")
```

### Run full pipeline programmatisch
```python
pipeline = ETLPipeline()
pipeline.run_full_pipeline(source='all')
```

### Command line usage
```powershell
# Extract alleen
python main.py --stage extract --source consignor

# Transform alleen  
python main.py --stage transform --source all

# Load alleen
python main.py --stage load --source eu_mrv

# Complete pipeline
python main.py --stage full --source all
```

## 🤝 Contributing

1. Fork het project
2. Maak een feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit je changes (`git commit -m 'Add some AmazingFeature'`)
4. Push naar de branch (`git push origin feature/AmazingFeature`)
5. Open een Pull Request

## 📋 Requirements

- Python 3.8+
- SQL Server 2017+ (met ODBC Driver 17)
- Windows 10+ (voor ODBC)
- 4GB+ RAM (voor data processing)

## 📄 License

Dit project is gelicenseerd onder de MIT License - zie het [LICENSE](LICENSE) bestand voor details.

## 🏫 Origineel Project

Gebaseerd op het Business Intelligence project van de opleiding, oorspronkelijk geïmplementeerd met SSIS en Visual Studio. Deze moderne Python implementatie brengt de ETL pipeline naar state-of-the-art data engineering practices met focus op:

- **Maintainability** - Clean code en modulaire architectuur
- **Scalability** - Pandas en SQL-based processing
- **Observability** - Comprehensive logging en monitoring
- **Testability** - Unit tests en data quality checks
- **Flexibility** - Configuration-driven pipelines
