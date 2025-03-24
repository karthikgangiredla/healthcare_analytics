from google.cloud import bigquery
import os

from dotenv import load_dotenv
load_dotenv()

# Set up your service account credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

# Project and dataset details
project_id = os.getenv("project_id")
dataset_id = os.getenv("dataset_id")
client = bigquery.Client(project=project_id)
# Dashboard views
dashboard_views = {
    "view_provider_dashboard": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_provider_dashboard` AS
        SELECT
          p.provider_id,
          p.specialty,
          COUNT(f.encounter_id) AS total_encounters,
          ROUND(AVG(f.duration_days), 2) AS avg_duration,
          ROUND(AVG(f.num_conditions), 2) AS avg_conditions,
          ROUND(AVG(f.num_procedures), 2) AS avg_procedures
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_provider` p
          ON f.provider_id = p.provider_id
        GROUP BY p.provider_id, p.specialty
        ORDER BY total_encounters DESC;
    """,

    "view_patient_dashboard": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_patient_dashboard` AS
        SELECT
          p.patient_id,
          p.gender,
          p.approx_age,
          p.is_alive,
          COUNT(f.encounter_id) AS total_encounters,
          ROUND(AVG(f.num_conditions), 2) AS avg_conditions
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_patient` p
          ON f.patient_id = p.patient_id
        GROUP BY p.patient_id, p.gender, p.approx_age, p.is_alive
        ORDER BY total_encounters DESC;
    """,

    "view_organization_dashboard": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_organization_dashboard` AS
        SELECT
          o.org_id,
          o.org_name,
          o.city,
          o.state,
          COUNT(f.encounter_id) AS total_encounters,
          ROUND(AVG(f.duration_days), 2) AS avg_duration,
          ROUND(AVG(f.num_procedures), 2) AS avg_procedures
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_organization` o
          ON f.org_id = o.org_id
        GROUP BY o.org_id, o.org_name, o.city, o.state
        ORDER BY total_encounters DESC;
    """
}

# Run all view creation queries
for name, sql in dashboard_views.items():
    print(f"Creating dashboard view: {name}...")
    job = client.query(sql)
    job.result()
    print(f" Created: {name}")

print("All dashboard views created successfully.")
