from google.cloud import bigquery
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

project_id = "healthcareanalysis-454507"
dataset_id = "healthcare_db"
client = bigquery.Client(project=project_id)

kpi_views = {
    "view_kpi_summary": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_kpi_summary` AS
        SELECT
          COUNT(DISTINCT patient_id) AS total_patients,
          COUNT(DISTINCT provider_id) AS total_providers,
          COUNT(*) AS total_encounters,
          ROUND(AVG(duration_days), 2) AS avg_encounter_duration,
          ROUND(AVG(num_conditions), 2) AS avg_conditions_per_encounter
        FROM `{project_id}.{dataset_id}.fact_encounters`;
    """,

    "view_top_providers": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_top_providers` AS
        SELECT
          f.provider_id,
          d.specialty,
          COUNT(*) AS total_encounters
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_provider` d
          ON f.provider_id = d.provider_id
        GROUP BY f.provider_id, d.specialty
        ORDER BY total_encounters DESC
        LIMIT 5;
    """,

    "view_monthly_trends": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_monthly_trends` AS
        SELECT
          EXTRACT(YEAR FROM start_date) AS year,
          EXTRACT(MONTH FROM start_date) AS month,
          COUNT(*) AS total_encounters
        FROM `{project_id}.{dataset_id}.fact_encounters`
        GROUP BY year, month
        ORDER BY year, month;
    """,

    "view_top_specialties_by_encounters": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_top_specialties_by_encounters` AS
        SELECT
          d.specialty,
          COUNT(*) AS total_encounters,
          ROUND(AVG(f.num_conditions), 2) AS avg_conditions
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_provider` d
          ON f.provider_id = d.provider_id
        GROUP BY d.specialty
        ORDER BY total_encounters DESC
        LIMIT 10;
    """,

    "view_encounters_by_state": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_encounters_by_state` AS
        SELECT
          o.state,
          COUNT(*) AS total_encounters
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_organization` o
          ON f.org_id = o.org_id
        GROUP BY o.state
        ORDER BY total_encounters DESC;
    """,

    "view_cost_distribution_by_age_group": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_cost_distribution_by_age_group` AS
        SELECT
          CASE
            WHEN p.approx_age < 18 THEN '0-17'
            WHEN p.approx_age BETWEEN 18 AND 34 THEN '18-34'
            WHEN p.approx_age BETWEEN 35 AND 49 THEN '35-49'
            WHEN p.approx_age BETWEEN 50 AND 64 THEN '50-64'
            ELSE '65+'
          END AS age_group,
          COUNT(*) AS encounter_count
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_patient` p
          ON f.patient_id = p.patient_id
        GROUP BY age_group
        ORDER BY age_group;
    """,

    "view_provider_condition_load": f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.view_provider_condition_load` AS
        SELECT
          f.provider_id,
          d.specialty,
          SUM(f.num_conditions) AS total_conditions,
          COUNT(*) AS total_encounters,
          ROUND(SUM(f.num_conditions) / COUNT(*), 2) AS avg_conditions_per_encounter
        FROM `{project_id}.{dataset_id}.fact_encounters` f
        JOIN `{project_id}.{dataset_id}.dim_provider` d
          ON f.provider_id = d.provider_id
        GROUP BY f.provider_id, d.specialty
        ORDER BY total_conditions DESC
        LIMIT 10;
    """
}

# Execute views
for name, sql in kpi_views.items():
    print(f"Creating KPI view: {name}...")
    job = client.query(sql)
    job.result()
    print(f"✅ Created: {name}")

print("✅ All KPI views created successfully.")
