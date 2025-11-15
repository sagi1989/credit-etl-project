import argparse

from repositories.db import (
    get_default_rate_by_income_type,
    get_default_rate_by_education_type,
    get_default_rate_by_age_band,
    get_default_rate_by_family_status,
    get_default_rate_by_housing_type,
    get_default_rate_by_contract_type,
)


def main():
    parser = argparse.ArgumentParser(description="ETL Reports CLI")

    parser.add_argument(
        "--report",
        type=str,
        required=True,
        help="Report name: income / education / age / family / housing / contract",
    )

    args = parser.parse_args()

    if args.report == "income":
        df = get_default_rate_by_income_type()
        print("\n=== Default rate by INCOME TYPE ===")
        print(df.to_string(index=False))

    elif args.report == "education":
        df = get_default_rate_by_education_type()
        print("\n=== Default rate by EDUCATION TYPE ===")
        print(df.to_string(index=False))

    elif args.report == "age":
        df = get_default_rate_by_age_band()
        print("\n=== Default rate by AGE BAND ===")
        print(df.to_string(index=False))

    elif args.report == "family":
        df = get_default_rate_by_family_status()
        print("\n=== Default rate by FAMILY STATUS ===")
        print(df.to_string(index=False))

    elif args.report == "housing":
        df = get_default_rate_by_housing_type()
        print("\n=== Default rate by HOUSING TYPE ===")
        print(df.to_string(index=False))

    elif args.report == "contract":
        df = get_default_rate_by_contract_type()
        print("\n=== Default rate by CONTRACT TYPE ===")
        print(df.to_string(index=False))

    else:
        print("Unknown report name. Please choose: income / education / age / family / housing / contract")


if __name__ == "__main__":
    main()
