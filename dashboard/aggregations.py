from google.cloud import bigquery
import os

# Set up your service account credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

# Project and dataset details
project_id = "healthcareanalysis-454507"
dataset_id = "healthcare_db"
client = bigquery.Client(project=project_id)

# Define SQL for each aggregation table
agg_queries = {
    "agg_provider_performance": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_provider_performance` AS
        SELECT
          provider_id,
          COUNT(*) AS total_encounters,
          ROUND(AVG(duration_days), 2) AS avg_duration,
          ROUND(AVG(num_conditions), 2) AS avg_conditions,
          ROUND(AVG(num_procedures), 2) AS avg_procedures,
        FROM `{project_id}.{dataset_id}.fact_encounters`
        GROUP BY provider_id;
    """,

    "agg_monthly_encounters": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.agg_monthly_encounters` AS
        SELECT
          EXTRACT(YEAR FROM start_date) AS year,
          EXTRACT(MONTH FROM start_date) AS month,
          COUNT(*) AS total_encounters,
        FROM `{project_id}.{dataset_id}.fact_encounters`
        GROUP BY year, month
        ORDER BY year, month;
    """
}

# Run each aggregation query
for name, sql in agg_queries.items():
    print(f"Creating aggregation table: {name}...")
    job = client.query(sql)
    job.result()
    print(f"✅ Created: {name}")

print("✅ All aggregation tables created.")
