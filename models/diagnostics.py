from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_OUT = PROJECT_ROOT / "data" / "out"


def plot_correlation_matrix(df: pd.DataFrame, filename: str = "correlation_matrix.jpg"):
    DATA_OUT.mkdir(parents=True, exist_ok=True)
    save_path = DATA_OUT / filename

    cor = df.corr(numeric_only=True)

    plt.figure(figsize=(10, 10))

    sns.heatmap(
        cor,
        annot=True,
        fmt=".2f",
        annot_kws={"size": 7},
        square=True,
    )

    plt.title("Correlation Matrix")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()

    print(f"[plot] correlation matrix saved to: {save_path}")
