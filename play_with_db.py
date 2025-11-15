from repositories.db import (
    get_default_rate_by_income_type,
    get_default_rate_by_education_type,
    get_default_rate_by_age_band,
)


def main():
    # תבחר איזה דוח אתה רוצה להריץ ע"י ביטול/הוספת הערות:

    print("=== Default rate by income type ===")
    df_income = get_default_rate_by_income_type()
    print(df_income.to_string(index=False))
    print()

    print("=== Default rate by education type ===")
    df_edu = get_default_rate_by_education_type()
    print(df_edu.to_string(index=False))
    print()

    print("=== Default rate by age band ===")
    df_age = get_default_rate_by_age_band()
    print(df_age.to_string(index=False))


if __name__ == "__main__":
    main()
