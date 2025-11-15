from pathlib import Path
from datetime import datetime
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_OUT = PROJECT_ROOT / "data" / "out"


def generate_etl_summary(
        df_before: pd.DataFrame,
        df_after: pd.DataFrame,
        duplicates_removed: int,
        output_filename: str = "etl_summary.txt"
    ):
    """
    Creates a summary report for the ETL run and saves it to data/out.
    """

    DATA_OUT.mkdir(parents=True, exist_ok=True)
    out_path = DATA_OUT / output_filename

    missing_counts = df_before.isna().sum()
    total_nans_before = int(missing_counts.sum())

    lines = []
    lines.append("ETL SUMMARY REPORT\n")
    lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    lines.append("=== GENERAL ===\n")
    lines.append(f"Rows before cleaning : {len(df_before):,}\n")
    lines.append(f"Rows after cleaning  : {len(df_after):,}\n")
    lines.append(f"Duplicates removed   : {duplicates_removed:,}\n")
    lines.append(f"Total NaN in input   : {total_nans_before:,}\n\n")

    lines.append("=== COLUMNS WITH NaN ===\n")
    for col, count in missing_counts.items():
        if count > 0:
            lines.append(f"- {col}: {count:,} missing\n")

    if missing_counts.sum() == 0:
        lines.append("No missing values detected.\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"[report] summary saved to: {out_path}")

    return out_path
