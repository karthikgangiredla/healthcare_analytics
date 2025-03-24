import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import plotly.express as px
from dotenv import load_dotenv
load_dotenv()
# Set up your service account credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"
# Project and dataset details
project_id = os.getenv("project_id")
dataset_id = os.getenv("dataset_id")
client = bigquery.Client(project=project_id)
# UI Setup
st.set_page_config(page_title="Healthcare Analytics Dashboard", layout="wide")
st.title(" Healthcare Provider Analytics Dashboard")

# Sidebar Navigation
st.sidebar.title(" Navigation")
navigation_options = [
    "Provider Dashboard", "Patient Dashboard", "Organization Dashboard", "Advanced Analytics", "Insights",
    "Table Explorer"
]
selected_nav = st.sidebar.radio("Select Section:", navigation_options)

# Table options for explorer
table_options = [
    "view_provider_dashboard", "view_patient_dashboard", "view_organization_dashboard",
    "agg_provider_performance", "agg_monthly_encounters",
    "mart_provider_productivity", "mart_patient_demographics",
    "view_kpi_summary", "view_top_providers", "view_monthly_trends",
    "view_top_specialties_by_encounters", "view_encounters_by_state",
    "view_cost_distribution_by_age_group", "view_provider_condition_load"
]

@st.cache_data(show_spinner=False)
def preview_table(view):
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{view}` LIMIT 1000"
    return client.query(query).to_dataframe()

@st.cache_data(show_spinner=False)
def load_data(view):
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{view}`"
    return client.query(query).to_dataframe()

provider_df = load_data("view_provider_dashboard")
patient_df = load_data("view_patient_dashboard")
org_df = load_data("view_organization_dashboard")
fact_df = load_data("fact_encounters")

# ============== NAVIGATION HANDLER ==============
if selected_nav == "Provider Dashboard":
    st.subheader(" Provider Performance")
    specialty = st.selectbox("Filter by Specialty", provider_df["specialty"].unique())
    filtered = provider_df[provider_df["specialty"] == specialty]
    st.metric("Total Providers", len(filtered))
    st.plotly_chart(px.bar(filtered.sort_values("total_encounters", ascending=False).head(10),
                           x="provider_id", y="total_encounters", title="Top 10 Providers by Encounter Volume"),
                    use_container_width=True)
    st.dataframe(filtered, use_container_width=True)

elif selected_nav == "Patient Dashboard":
    st.subheader("👥 Patient Insights")
    age_range = st.slider("Select Age Range", 0, 100, (0, 100))
    patient_filtered = patient_df[(patient_df["approx_age"] >= age_range[0]) & (patient_df["approx_age"] <= age_range[1])]
    st.plotly_chart(px.histogram(patient_filtered, x="approx_age", nbins=20, title="Patient Age Distribution"), use_container_width=True)
    st.plotly_chart(px.pie(patient_filtered, names="gender", title="Patient Gender Split"), use_container_width=True)
    st.dataframe(patient_filtered, use_container_width=True)

elif selected_nav == "Organization Dashboard":
    st.subheader(" Organization Analysis")
    selected_state = st.selectbox("Select State", org_df["state"].unique())
    org_filtered = org_df[org_df["state"] == selected_state]
    st.plotly_chart(px.bar(org_filtered.sort_values("total_encounters", ascending=False).head(10),
                           x="org_name", y="total_encounters", title="Top Organizations by Encounter Volume"),
                    use_container_width=True)
    org_line = org_filtered[(org_filtered["avg_duration"] > 0) & (org_filtered["avg_procedures"] > 0)]
    if not org_line.empty:
        st.plotly_chart(px.line(org_line.sort_values("avg_duration"),
                                x="avg_duration", y="avg_procedures", color="city",
                                title="Procedure vs Duration by Organization (Line Chart)"),
                        use_container_width=True)
    else:
        st.info("No data available for line chart.")
    st.dataframe(org_filtered, use_container_width=True)

elif selected_nav == "Advanced Analytics":
    st.subheader(" Advanced Condition & Age Analysis")
    fact_patient_df = fact_df.merge(patient_df, on="patient_id")
    fact_patient_df["age_group"] = pd.cut(fact_patient_df["approx_age"],
                                          bins=[0, 17, 34, 49, 64, 200],
                                          labels=["0-17", "18-34", "35-49", "50-64", "65+"])
    age_group_stats = fact_patient_df.groupby("age_group").agg(
        total_encounters=("encounter_id", "count"),
        avg_conditions=("num_conditions", "mean")
    ).reset_index()
    st.plotly_chart(px.bar(age_group_stats, x="age_group", y="avg_conditions",
                           title="Average Conditions by Age Group"), use_container_width=True)
    st.plotly_chart(px.pie(age_group_stats, names="age_group", values="total_encounters",
                           title="Encounters by Age Group"), use_container_width=True)
    st.dataframe(age_group_stats, use_container_width=True)

elif selected_nav == "Insights":
    st.subheader(" Demographic & Time-Based Insights")
    fact_patient_df = fact_df.merge(patient_df, on="patient_id")
    fact_patient_df["age_group"] = pd.cut(fact_patient_df["approx_age"],
                                          bins=[0, 17, 34, 49, 64, 200],
                                          labels=["0-17", "18-34", "35-49", "50-64", "65+"])
    condition_by_demo = fact_patient_df.groupby(["age_group", "gender"]).agg(
        avg_conditions=("num_conditions", "mean")
    ).reset_index()
    st.plotly_chart(px.bar(condition_by_demo, x="age_group", y="avg_conditions", color="gender",
                           barmode="group", title="Avg Conditions by Age Group & Gender"),
                    use_container_width=True)
    fact_df["month"] = pd.to_datetime(fact_df["start_date"]).dt.to_period("M").astype(str)
    monthly_trends = fact_df.groupby("month").agg(encounter_count=("encounter_id", "count")).reset_index()
    st.plotly_chart(px.line(monthly_trends, x="month", y="encounter_count", title="Encounter Trends Over Time"),
                    use_container_width=True)
    st.dataframe(condition_by_demo, use_container_width=True)
    st.dataframe(monthly_trends, use_container_width=True)

elif selected_nav == "Table Explorer":
    st.subheader(" Table Explorer")
    selected_table = st.selectbox("Choose Table to Preview", table_options)
    if selected_table:
        st.dataframe(preview_table(selected_table), use_container_width=True)
