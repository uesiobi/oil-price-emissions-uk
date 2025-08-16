# This script for the ETL connects to Azure DB and loads the dataset using PYODBC
import pandas as pd
import pyodbc
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv() # loads Azure credentials from .env into environment
# Azure DB credentials
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DRIVER = os.getenv("DRIVER")

# First I tested the connection to ensure it can connect to Azure DB before loading csv, I use try..except error handling
try:
    conn = pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True  # speeds up bulk inserts
    print("✅ Connection Successful!")
except pyodbc.Error as e:
    print("❌ Error connecting to Database:", e) # also prints 'e' the error message

# this is a helper to Create table if not exists
def create_table_from_df(cursor, table_name, df):
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            sql_type = "INT"
        elif "float" in str(dtype):
            sql_type = "FLOAT"
        elif "datetime" in str(dtype):
            sql_type = "DATETIME"
        else:
            sql_type = "NVARCHAR(MAX)"
        cols.append(f"[{col}] {sql_type}")
    sql = f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE [{table_name}] ({', '.join(cols)});"
    cursor.execute(sql)
    cursor.commit()

# Defining the path where I have the csv file to be loaded
current_path = Path(__file__).resolve().parent
while not (current_path / "data" / "cleaned").exists() and current_path.parent != current_path:
    current_path = current_path.parent
CLEANED_DIR = current_path / "data" / "cleaned"

datasets = {
    "crude_oil": CLEANED_DIR / "crude_oil_cleaned.csv",
    "emissions": CLEANED_DIR / "uk_emissions_cleaned.csv",
    "fossil_fuel": CLEANED_DIR / "fossil_fuel_cleaned.csv"
}

# Loading CSVs into SQL Server
for table_name, file_path in datasets.items():
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        continue

    df = pd.read_csv(file_path)

    #  Create table automatically
    create_table_from_df(cursor, table_name, df)

    # Prepare insert statement
    columns = ",".join(f"[{c}]" for c in df.columns)
    placeholders = ",".join("?" * len(df.columns))
    sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"

    #Insert all rows
    cursor.executemany(sql, df.values.tolist())
    conn.commit()
    print(f"✅ Loaded '{table_name}' ({len(df)} rows) into SQL Server.")

# closing connection after loading the data.
cursor.close()
conn.close()
print(" All cleaned datasets loaded successfully into SQL Server!")
