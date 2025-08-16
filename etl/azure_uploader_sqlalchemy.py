# This script for the ETL connects to Azure DB and loads the dataset using SQLAlchemy
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import urllib
import os
from dotenv import load_dotenv

load_dotenv() # loads Azure credentials from .env into environment
# Azure DB credentials
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DRIVER = os.getenv("DRIVER")

# SQLAlchemy Engine
params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASS};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Path definition
current_path = Path(__file__).resolve().parent
while not (current_path / "data" / "cleaned").exists() and current_path.parent != current_path:
    current_path = current_path.parent
CLEANED_DIR = current_path / "data" / "cleaned"

datasets = {
    "crude_oil": CLEANED_DIR / "crude_oil_cleaned.csv",
    "emissions": CLEANED_DIR / "uk_emissions_cleaned.csv",
    "fossil_fuel": CLEANED_DIR / "fossil_fuel_cleaned.csv"
}

# Loading the csv to the Azure sql server
for table_name, file_path in datasets.items():
    if not file_path.exists():
        print(f" File not found: {file_path}")
        continue

    df = pd.read_csv(file_path)

    # Use pandas.to_sql to automatically create table if it doesn't exist
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",  # options: 'fail', 'replace', 'append'
        index=False,
        chunksize=1000,  # optional: helps with large datasets
    )
    print(f" Loaded '{table_name}' ({len(df)} rows) into SQL Server.")

print(" All cleaned datasets loaded successfully into SQL Server!")
