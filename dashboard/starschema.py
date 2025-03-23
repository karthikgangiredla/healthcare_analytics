from google.cloud import bigquery
import os

# Set credentials (make sure this is configured correctly)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

# Setup
project_id = "healthcareanalysis-454507"
dataset_id = "healthcare_db"
client = bigquery.Client(project=project_id)

# SQL for all tables
sql_statements = {
    "fact_encounters": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.fact_encounters` AS
        SELECT
          ENCOUNTER_ID AS encounter_id,
          PATIENT AS patient_id,
          PROVIDER AS provider_id,
          ORGANIZATION AS org_id,
          DATE(START) AS start_date,
          DATE(STOP) AS stop_date,
          DURATION_DAYS AS duration_days,
          NUM_CONDITIONS AS num_conditions,
          NUM_OBSERVATIONS AS num_observations,
          NUM_PROCEDURES AS num_procedures,
        FROM `{project_id}.{dataset_id}.transformed_encounters`;
    """,

    "dim_patient": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_patient` AS
        SELECT DISTINCT
          PATIENT AS patient_id,
          PATIENT_GENDER AS gender,
          AGE_AT_ENCOUNTER AS approx_age,
          PATIENT_IS_ALIVE AS is_alive
        FROM `{project_id}.{dataset_id}.transformed_encounters`;
    """,

    "dim_provider": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_provider` AS
        SELECT DISTINCT
          PROVIDER AS provider_id,
          SPECIALTY AS specialty
        FROM `{project_id}.{dataset_id}.transformed_encounters`;
    """,

    "dim_organization": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_organization` AS
        SELECT DISTINCT
          ORGANIZATION AS org_id,
          NAME AS org_name,
          ADDRESS,
          CITY,
          STATE
        FROM `{project_id}.{dataset_id}.transformed_encounters`;
    """,

    "dim_time": f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_time` AS
        SELECT DISTINCT
          DATE(START) AS date,
          EXTRACT(YEAR FROM DATE(START)) AS year,
          EXTRACT(MONTH FROM DATE(START)) AS month,
          EXTRACT(DAY FROM DATE(START)) AS day,
          FORMAT_DATE('%B', DATE(START)) AS month_name,
          FORMAT_DATE('%A', DATE(START)) AS day_name
        FROM `{project_id}.{dataset_id}.transformed_encounters`;
    """
}

# Run all SQL statements
for name, sql in sql_statements.items():
    print(f"Creating table: {name}...")
    job = client.query(sql)
    job.result()
    print(f"✅ {name} created.")

print("✅ All fact and dimension tables created successfully.")
