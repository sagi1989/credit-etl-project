from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_OUT = PROJECT_ROOT / "data" / "out"

COLS_OF_INTEREST = [
    "TARGET", "NAME_CONTRACT_TYPE", "CODE_GENDER",
    "FLAG_OWN_CAR", "FLAG_OWN_REALTY", "CNT_CHILDREN",
    "AMT_INCOME_TOTAL", "AMT_CREDIT", "NAME_INCOME_TYPE",
    "NAME_EDUCATION_TYPE", "NAME_FAMILY_STATUS", "NAME_HOUSING_TYPE",
    "DAYS_BIRTH", "DAYS_EMPLOYED", "REGION_RATING_CLIENT_W_CITY",
    "WEEKDAY_APPR_PROCESS_START", "HOUR_APPR_PROCESS_START",
    "DEF_30_CNT_SOCIAL_CIRCLE", "OCCUPATION_TYPE", "ORGANIZATION_TYPE",
    "CNT_FAM_MEMBERS",
]


# ----------------------------- Extract ----------------------------- #
def load_raw_application(filename: str = "application_data.csv") -> pd.DataFrame:
    path = DATA_RAW / filename
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path, index_col="SK_ID_CURR", low_memory=False)
    df.index = df.index.astype(str)
    print(f"[load] shape: {df.shape}")
    return df


# ----------------------------- Transform ----------------------------- #
def transform_application(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    existing = [c for c in COLS_OF_INTEREST if c in df.columns]
    new_app = df[existing].copy()

    print(f"[transform] shape before cleaning: {new_app.shape}")
    print(new_app.head(2))
    print(new_app.describe(include="all").transpose())

    # Fill NaN
    new_app = new_app.fillna(0)
    print("[transform] NaN removed")

    # Drop duplicates
    before = len(new_app)
    new_app = new_app.drop_duplicates()
    duplicates_removed = before - len(new_app)
    print(f"[transform] removed {duplicates_removed} duplicates")

    # Add categorical target
    if "TARGET" in new_app.columns:
        new_app["TARGET_CAT"] = new_app["TARGET"].replace(
            {0: "NO_DEFAULT", 1: "DEFAULT"}
        )

    if "DAYS_BIRTH" in new_app.columns:
        new_app["AGE"] = (-new_app["DAYS_BIRTH"] / 365).astype(int)
        new_app = new_app.drop(columns=["DAYS_BIRTH"])

    if "DAYS_EMPLOYED" in new_app.columns:
        new_app["DAYS_EMPLOYED_ANOM"] = new_app["DAYS_EMPLOYED"] == 365243
        new_app.loc[new_app["DAYS_EMPLOYED"] == 365243, "DAYS_EMPLOYED"] = pd.NA
        new_app["YEARS_EMPLOYED"] = (-new_app["DAYS_EMPLOYED"] / 365).astype("float")
        new_app = new_app.drop(columns=["DAYS_EMPLOYED"])

    print(f"[transform] shape after cleaning: {new_app.shape}")
    return new_app, duplicates_removed


def save_transformed(df: pd.DataFrame, filename: str = "new_application.csv") -> Path:
    DATA_OUT.mkdir(parents=True, exist_ok=True)
    out_path = DATA_OUT / filename
    df = df.copy()
    df["SK_ID_CURR"] = df.index
    cols = ["SK_ID_CURR"] + [c for c in df.columns if c != "SK_ID_CURR"]
    df = df[cols]
    df.to_csv(out_path, index=False)
    print(f"[load] saved to: {out_path}")
    return out_path
