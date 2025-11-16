from models.cleaning import (
    load_raw_application,
    transform_application,
    save_transformed,
)
from models.diagnostics import plot_correlation_matrix
from utils.report import generate_etl_summary
from repositories.db import (
    save_to_sqlite,
    get_default_rate_by_income_type,
    get_default_rate_by_education_type,
    get_default_rate_by_age_band,
    get_default_rate_by_family_status,
    get_default_rate_by_housing_type,
    get_default_rate_by_contract_type,
)


def run_etl():
    """Run the full ETL pipeline: extract, transform, load, diagnostics, summary."""
    # Extract
    df_before = load_raw_application()

    # Transform
    cleaned, duplicates_removed = transform_application(df_before)

    # Load to CSV
    save_transformed(cleaned)

    # Load to SQLite
    db_path = save_to_sqlite(cleaned)

    # Diagnostics
    plot_correlation_matrix(cleaned)

    # Summary report (text file in data/out)
    generate_etl_summary(
        df_before=df_before,
        df_after=cleaned,
        duplicates_removed=duplicates_removed,
    )

    print(f"[load] SQLite DB saved to: {db_path}")
    print("[etl] ETL pipeline completed successfully.")


def interactive_menu():
    """Simple interactive menu to select and print analytics reports from the DB."""
    while True:
        answer = input("\nDo you want to generate a report from the DB? (y/n): ").strip().lower()
        if answer not in ("y", "yes"):
            print("Exiting interactive menu.")

            # Ask if the user wants to launch the dashboard
            open_dash = input("\nDo you want to open the dashboard? (y/n): ").strip().lower()

            if open_dash in ("y", "yes"):
                # Import here to keep changes minimal and localized
                import subprocess
                import sys
                from pathlib import Path

                project_root = Path(__file__).resolve().parent
                dashboard_path = project_root / "dashboard.py"

                print("\nLaunching Streamlit dashboard...")
                print("If the browser does not open automatically, go to http://localhost:8501")

                subprocess.run(
                    [sys.executable, "-m", "streamlit", "run", str(dashboard_path)],
                    cwd=project_root,
                )
            else:
                print("\nYou can open the dashboard later by running:")
                print("  streamlit run dashboard.py")

            break

        print(
            "\nChoose a report:\n"
            " 1) income    - default rate by income type\n"
            " 2) education - default rate by education type\n"
            " 3) age       - default rate by age band\n"
            " 4) family    - default rate by family status\n"
            " 5) housing   - default rate by housing type\n"
            " 6) contract  - default rate by contract type\n"
        )

        choice = input("Enter a number (1–6): ").strip()

        if choice == "1":
            df = get_default_rate_by_income_type()
            title = "Default rate by INCOME TYPE"
        elif choice == "2":
            df = get_default_rate_by_education_type()
            title = "Default rate by EDUCATION TYPE"
        elif choice == "3":
            df = get_default_rate_by_age_band()
            title = "Default rate by AGE BAND"
        elif choice == "4":
            df = get_default_rate_by_family_status()
            title = "Default rate by FAMILY STATUS"
        elif choice == "5":
            df = get_default_rate_by_housing_type()
            title = "Default rate by HOUSING TYPE"
        elif choice == "6":
            df = get_default_rate_by_contract_type()
            title = "Default rate by CONTRACT TYPE"
        else:
            print("Invalid choice, please try again.")
            continue

        print(f"\n=== {title} ===")
        print(df.to_string(index=False))


if __name__ == "__main__":
    run_etl()
    interactive_menu()
