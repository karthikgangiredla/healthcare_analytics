import pandas as pd

# Load necessary CSVs
patients_df = pd.read_csv("data/patients.csv", parse_dates=["BIRTHDATE", "DEATHDATE"])
encounters_df = pd.read_csv("data/encounters.csv", parse_dates=["START", "STOP"])
conditions_df = pd.read_csv("data/conditions.csv", parse_dates=["START", "STOP"])
providers_df = pd.read_csv("data/providers.csv").rename(columns={"Id": "PROVIDER_ID", "SPECIALITY": "SPECIALTY"})
organizations_df = pd.read_csv("data/organizations.csv").rename(columns={"Id": "ORG_ID"})
observations_df = pd.read_csv("data/observations.csv")
procedures_df = pd.read_csv("data/procedures.csv")
claims_txn_df = pd.read_csv("data/claims_transactions.csv")

# Clean timezone issues
for col in ["START", "STOP"]:
    encounters_df[col] = encounters_df[col].dt.tz_localize(None)
    conditions_df[col] = conditions_df[col].dt.tz_localize(None)

patients_df["BIRTHDATE"] = patients_df["BIRTHDATE"].dt.tz_localize(None)
patients_df["DEATHDATE"] = patients_df["DEATHDATE"].dt.tz_localize(None)

# --- Feature Engineering ---
encounters_df.rename(columns={"Id": "ENCOUNTER_ID"}, inplace=True)
# Merge BIRTHDATE to compute AGE
encounters_df = encounters_df.merge(patients_df[["Id", "BIRTHDATE"]], left_on="PATIENT", right_on="Id", how="left")
encounters_df["AGE_AT_ENCOUNTER"] = (encounters_df["START"] - encounters_df["BIRTHDATE"]).dt.days // 365
encounters_df["DURATION_DAYS"] = (encounters_df["STOP"] - encounters_df["START"]).dt.days

# Number of conditions per encounter
condition_counts = conditions_df.groupby("ENCOUNTER").size().reset_index(name="NUM_CONDITIONS")
encounters_df = encounters_df.merge(condition_counts, left_on="ENCOUNTER_ID", right_on="ENCOUNTER", how="left")
encounters_df["NUM_CONDITIONS"] = encounters_df["NUM_CONDITIONS"].fillna(0)

# Rename encounter ID for clarity


# Add provider specialty
encounters_df = encounters_df.merge(providers_df[["PROVIDER_ID", "SPECIALTY"]],
                                    left_on="PROVIDER", right_on="PROVIDER_ID", how="left")

# Add organization info
encounters_df = encounters_df.merge(organizations_df[["ORG_ID", "NAME", "ADDRESS", "CITY", "STATE"]],
                                    left_on="ORGANIZATION", right_on="ORG_ID", how="left")

# Add claims cost per encounter (if available)
if "ENCOUNTER" in claims_txn_df.columns and "COST" in claims_txn_df.columns:
    cost_summary = claims_txn_df.groupby("ENCOUNTER")["COST"].sum().reset_index()
    encounters_df = encounters_df.merge(cost_summary, left_on="ENCOUNTER_ID", right_on="ENCOUNTER", how="left")
    encounters_df["COST"] = encounters_df["COST"].fillna(0)
else:
    encounters_df["COST"] = 0

# Observation and procedure counts
obs_counts = observations_df.groupby("ENCOUNTER").size().reset_index(name="NUM_OBSERVATIONS")
encounters_df = encounters_df.merge(obs_counts, left_on="ENCOUNTER_ID", right_on="ENCOUNTER", how="left")
encounters_df["NUM_OBSERVATIONS"] = encounters_df["NUM_OBSERVATIONS"].fillna(0)

proc_counts = procedures_df.groupby("ENCOUNTER").size().reset_index(name="NUM_PROCEDURES")
encounters_df = encounters_df.merge(proc_counts, left_on="ENCOUNTER_ID", right_on="ENCOUNTER", how="left")
encounters_df["NUM_PROCEDURES"] = encounters_df["NUM_PROCEDURES"].fillna(0)

# Provider encounter volume
provider_counts = encounters_df.groupby("PROVIDER")["ENCOUNTER_ID"].count().reset_index(name="PROVIDER_TOTAL_ENCOUNTERS")
encounters_df = encounters_df.merge(provider_counts, on="PROVIDER", how="left")

# Add gender and alive status
patients_df["IS_ALIVE"] = patients_df["DEATHDATE"].isna().astype(int)
encounters_df = encounters_df.merge(patients_df[["Id", "GENDER", "IS_ALIVE"]], left_on="PATIENT", right_on="Id", how="left")
encounters_df.rename(columns={"GENDER": "PATIENT_GENDER", "IS_ALIVE": "PATIENT_IS_ALIVE"}, inplace=True)

# --- Final Output ---
final_df = encounters_df[[
    "ENCOUNTER_ID", "PATIENT", "PATIENT_GENDER", "AGE_AT_ENCOUNTER", "PATIENT_IS_ALIVE",
    "PROVIDER", "SPECIALTY", "PROVIDER_TOTAL_ENCOUNTERS",
    "ORGANIZATION", "NAME", "ADDRESS", "CITY", "STATE",
    "START", "STOP", "DURATION_DAYS",
    "NUM_CONDITIONS", "NUM_OBSERVATIONS", "NUM_PROCEDURES", "COST"
]]

# Save the transformed dataset
final_df.to_csv("data/transformdata/transformed_encounters.csv", index=False)
print(f"Transformed data saved to 'transformed_encounters.csv' with shape: {final_df.shape}")
print(final_df.head(18))