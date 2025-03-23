import pandas as pd
from pathlib import Path

file_paths = {
    "allergies": "data/allergies.csv",
    "careplans": "data/careplans.csv",
    "claims_transactions": "data/claims_transactions.csv",
    "conditions": "data/conditions.csv",
    "devices": "data/devices.csv",
    "encounters": "data/encounters.csv",
    "imaging_studies": "data/imaging_studies.csv",
    "immunizations": "data/immunizations.csv",
    "medications": "data/medications.csv",
    "observations": "data/observations.csv",
    "organizations": "data/organizations.csv",
    "patients": "data/patients.csv",
    "payer_transitions": "data/payer_transitions.csv",
    "payers": "data/payers.csv",
    "procedures": "data/procedures.csv",
    "providers": "data/providers.csv",
    "supplies": "data/supplies.csv",
    "claims": "data/claims.csv"
}

dataframes = {}
extract_summary = []

for name, path in file_paths.items():
    try:
        df = pd.read_csv(path)
        dataframes[name] = df
        extract_summary.append({
            "Table": name,
            "Columns": df.columns.tolist(),
            "Num Rows": len(df)
        })
    except Exception as e:
        extract_summary.append({
            "Table": name,
            "Columns": [],
            "Num Rows": 0,
            "Error": str(e)
        })
 
summary_df = pd.DataFrame(extract_summary)
print(summary_df.head(18))

