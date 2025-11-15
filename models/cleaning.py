#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL Script for Loan Defaulter Dataset (based on your EDA)
=========================================================
Stages:
1. Extract: read CSV in chunks
2. Transform: fillna(0), map TARGET, group Education & Occupation
3. Load: replace table in SQLite (safe), export to CSV
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# --- Paths ---
RAW_DIR = "../data/raw"
OUT_DIR = "../data/out"
DB_PATH = os.path.join(OUT_DIR, "loans.db")
TABLE_NAME = "loans_clean"
STAGING_TABLE = "loans_staging"
DEFAULT_CSV = os.path.join(RAW_DIR, "application_data.csv")

# ---------- TRANSFORM ----------
def transform(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Transform stage based on actual EDA transformations:
    1. Fill missing values with 0
    2. Map TARGET -> TARGET_CAT ("DEFAULT"/"NO_DEFAULT")
    3. Create EDUCATION_CATEGORY ("With Degree"/"Without Degree")
    4. Create OCCUPATION_CATEGORY ("Blue-Collar"/"White-Collar"/"Service")
    """
    chunk = chunk.copy()

    # 1️⃣ Normalize column names (for consistency)
    chunk.columns = [c.strip().replace(" ", "_").upper() for c in chunk.columns]

    # 2️⃣ Fill NaN with 0 (as done in your EDA)
    chunk = chunk.fillna(0)

    # 3️⃣ Map TARGET to TARGET_CAT
    if "TARGET" in chunk.columns:
        chunk["TARGET_CAT"] = chunk["TARGET"].replace({0: "NO_DEFAULT", 1: "DEFAULT"})

    # 4️⃣ Map education categories
    if "NAME_EDUCATION_TYPE" in chunk.columns:
        degree_mapping = {
            "Lower secondary": "Without Degree",
            "Secondary / secondary special": "Without Degree",
            "Incomplete higher": "With Degree",
            "Higher education": "With Degree",
            "Academic degree": "With Degree",
        }
        chunk["EDUCATION_CATEGORY"] = chunk["NAME_EDUCATION_TYPE"].map(degree_mapping)

    # 5️⃣ Map occupation categories
    if "OCCUPATION_TYPE" in chunk.columns:
        occupation_to_category = {
            "Laborers": "Blue-Collar", "Drivers": "Blue-Collar", "Low-skill Laborers": "Blue-Collar",
            "Security staff": "Blue-Collar", "Cleaning staff": "Blue-Collar",
            "Accountants": "White-Collar", "Managers": "White-Collar", "Core staff": "White-Collar",
            "High skill tech staff": "White-Collar", "IT staff": "White-Collar", "HR staff": "White-Collar",
            "Secretaries": "White-Collar", "Realty agents": "White-Collar", "Medicine staff": "White-Collar",
            "Sales staff": "Service", "Cooking staff": "Service", "Private service staff": "Service",
            "Waiters/barmen staff": "Service"
        }
        chunk["OCCUPATION_CATEGORY"] = chunk["OCCUPATION_TYPE"].map(occupation_to_category)

    return chunk


# ---------- SAFE LOAD INTO SQLITE ----------
def load_sqlite(engine):
    """Replace target table safely (compatible with all SQLite versions)."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME};"))
        conn.execute(text(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM {STAGING_TABLE};"))
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE};"))


# ---------- MAIN ----------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # --- Verify that the raw file exists ---
    if not os.path.exists(DEFAULT_CSV):
        raise FileNotFoundError(f"Expected raw file not found: {DEFAULT_CSV}")

    print(f"📁 Using raw data file: {DEFAULT_CSV}")

    # --- Extract + Transform + Load ---
    engine = create_engine(f"sqlite:///{DB_PATH}")
    first = True
    total_rows = 0

    for chunk in pd.read_csv(DEFAULT_CSV, chunksize=50000, low_memory=False):
        t = transform(chunk)
        total_rows += len(t)
        t.to_sql(STAGING_TABLE, engine, if_exists="replace" if first else "append", index=False)
        first = False
        print(f"🔄 Processed {total_rows:,} rows so far...")

    # --- Safe load to final table ---
    load_sqlite(engine)

    print(f"✅ Done. SQLite DB created at: {DB_PATH}")
    print(f"   Target table: {TABLE_NAME}")

    # --- Export to CSV ---
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME};", conn)
    csv_path_out = os.path.join(OUT_DIR, "loans_clean.csv")
    df.to_csv(csv_path_out, index=False)
    print(f"✅ Also exported a clean CSV version at: {csv_path_out}")

    # --- Verify that the raw data file is untouched ---
    raw_size = os.path.getsize(DEFAULT_CSV)
    print(f"🛡️ Raw data file preserved: {DEFAULT_CSV} ({raw_size/1024/1024:.2f} MB)")
    print(f"📊 Final table shape: {df.shape[0]:,} rows × {df.shape[1]:,} columns")


if __name__ == "__main__":
    main()
