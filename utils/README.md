# Credit Application ETL Project

This project implements a complete ETL pipeline in Python for credit application data.  
It includes data extraction, cleaning, feature engineering, loading into CSV and SQLite,
automated reports, a CLI interface, and a Streamlit dashboard.

---

## 🚀 Features

### **Extract**
- Load raw credit application data from CSV located in `data/raw/`.

### **Transform**
- Data cleaning and validation
- Handling missing values and anomalies
- Feature engineering:
  - `AGE` (derived from `DAYS_BIRTH`)
  - `YEARS_EMPLOYED` (derived from `DAYS_EMPLOYED`)
- Removing duplicates
- Standardizing column formats

### **Load**
- Save cleaned data to:
  - CSV (`data/processed/`)
  - SQLite database (`data/db/etl.sqlite3`)

### **Reports & Diagnostics**
- Summary text report
- Correlation matrix
- Additional analytical functions implemented in `repositories/db.py`

### **CLI Interface**
Run reports from the command line:

```bash
python -m service.cli --report summary
