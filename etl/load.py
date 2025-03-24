from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\karth\AppData\Roaming\gcloud\application_default_credentials.json"

project_id = "healthcareanalysis-454507"
dataset_id = "healthcare_db"
table_id = "transformed_encounters"
csv_file_path = "data/transformdata/transformed_encounters.csv"


client = bigquery.Client(project=project_id) #client

table_ref = f"{project_id}.{dataset_id}.{table_id}"#table reference


job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
)

with open(csv_file_path, "rb") as file:
    load_job = client.load_table_from_file(file, table_ref, job_config=job_config)

load_job.result()  

table = client.get_table(table_ref)
print(f" Loaded {table.num_rows} rows into {table_ref}")
