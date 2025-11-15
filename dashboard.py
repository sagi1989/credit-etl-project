import streamlit as st
import pandas as pd
from repositories.db import (
    get_default_rate_by_income_type,
    get_default_rate_by_education_type,
    get_default_rate_by_age_band,
    get_default_rate_by_family_status,
    get_default_rate_by_housing_type,
    get_default_rate_by_contract_type,
)

# Page title
st.title("Credit Risk Dashboard")
st.write("Interactive dashboard based on the cleaned SQLite DB.")

# Sidebar menu
st.sidebar.header("Select a report")

report = st.sidebar.selectbox(
    "Choose report:",
    [
        "Default rate by income type",
        "Default rate by education type",
        "Default rate by age band",
        "Default rate by family status",
        "Default rate by housing type",
        "Default rate by contract type",
    ]
)

# Get data based on selection
if report == "Default rate by income type":
    df = get_default_rate_by_income_type()
elif report == "Default rate by education type":
    df = get_default_rate_by_education_type()
elif report == "Default rate by age band":
    df = get_default_rate_by_age_band()
elif report == "Default rate by family status":
    df = get_default_rate_by_family_status()
elif report == "Default rate by housing type":
    df = get_default_rate_by_housing_type()
elif report == "Default rate by contract type":
    df = get_default_rate_by_contract_type()

# Show table
st.subheader(report)
st.dataframe(df)

# Simple bar chart if numeric column exists
if "default_rate" in df.columns:
    st.subheader("Default Rate Chart")
    st.bar_chart(df.set_index(df.columns[0])["default_rate"])
