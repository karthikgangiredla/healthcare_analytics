from google.cloud import bigquery
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

# Setup
project_id = "healthcareanalysis-454507"
dataset_id = "healthcare_db"
client = bigquery.Client(project=project_id)

# Define SQL for each data mart
mart_queries = {
    "mart_provider_productivity": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_provider_productivity` AS
        SELECT
          f.provider_id,
          p.specialty,
          COUNT(*) AS encounter_count,
          ROUND(AVG(f.duration_days), 2) AS avg_duration,
          ROUND(AVG(f.num_conditions), 2) AS avg_conditions
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_provider` p
          ON f.provider_id = p.provider_id
        GROUP BY f.provider_id, p.specialty;
    """,

    "mart_patient_demographics": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_patient_demographics` AS
        SELECT
          gender,
          is_alive,
          COUNT(*) AS patient_count,
          ROUND(AVG(approx_age), 2) AS avg_age
        FROM `{project_id}.{dataset_id}.dim_patient`
        GROUP BY gender, is_alive;
    """
}

# Execute each query
for name, sql in mart_queries.items():
    print(f"Creating data mart: {name}...")
    job = client.query(sql)
    job.result()
    print(f"✅ Created: {name}")

print("✅ All data marts created.")
