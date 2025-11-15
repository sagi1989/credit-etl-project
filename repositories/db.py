from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("data/out/applications.db")
TABLE_NAME = "applications"


def save_to_sqlite(
    df: pd.DataFrame,
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()
    return db_path


def get_default_rate_by_income_type(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:

    conn = sqlite3.connect(db_path)
    try:
        query = f"""
        SELECT
            NAME_INCOME_TYPE,
            COUNT(*) AS n_clients,
            AVG(TARGET) AS default_rate
        FROM {table_name}
        GROUP BY NAME_INCOME_TYPE
        ORDER BY default_rate DESC;
        """
        df_result = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df_result


def get_default_rate_by_education_type(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        query = f"""
        SELECT
            NAME_EDUCATION_TYPE,
            COUNT(*) AS n_clients,
            AVG(TARGET) AS default_rate
        FROM {table_name}
        GROUP BY NAME_EDUCATION_TYPE
        ORDER BY default_rate DESC;
        """
        df_result = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df_result


def get_default_rate_by_age_band(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        # נביא רק את העמודות AGE ו-TARGET
        query = f"""
        SELECT AGE, TARGET
        FROM {table_name}
        WHERE AGE IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    # הגדרת טווחי גיל
    bins = [18, 25, 35, 45, 55, 65, 120]
    labels = [
        "18-24",
        "25-34",
        "35-44",
        "45-54",
        "55-64",
        "65+",
    ]

    df["age_band"] = pd.cut(df["AGE"], bins=bins, labels=labels, right=False)

    grouped = (
        df.groupby("age_band", observed=True)["TARGET"]
            .agg(n_clients="count", default_rate="mean")
            .reset_index()
            .sort_values("default_rate", ascending=False)
    )

    return grouped

def get_default_rate_by_family_status(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    """
    שיעור דיפולט לפי מצב משפחתי (NAME_FAMILY_STATUS).
    """
    conn = sqlite3.connect(db_path)
    try:
        query = f"""
        SELECT
            NAME_FAMILY_STATUS,
            COUNT(*) AS n_clients,
            AVG(TARGET) AS default_rate
        FROM {table_name}
        GROUP BY NAME_FAMILY_STATUS
        ORDER BY default_rate DESC;
        """
        df_result = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df_result


def get_default_rate_by_housing_type(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    """
    שיעור דיפולט לפי סוג מגורים (NAME_HOUSING_TYPE).
    """
    conn = sqlite3.connect(db_path)
    try:
        query = f"""
        SELECT
            NAME_HOUSING_TYPE,
            COUNT(*) AS n_clients,
            AVG(TARGET) AS default_rate
        FROM {table_name}
        GROUP BY NAME_HOUSING_TYPE
        ORDER BY default_rate DESC;
        """
        df_result = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df_result


def get_default_rate_by_contract_type(
    db_path: Path = DB_PATH,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        query = f"""
        SELECT
            NAME_CONTRACT_TYPE,
            COUNT(*) AS n_clients,
            AVG(TARGET) AS default_rate
        FROM {table_name}
        GROUP BY NAME_CONTRACT_TYPE
        ORDER BY default_rate DESC;
        """
        df_result = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df_result
