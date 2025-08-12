# This is me performing Loading process; Loading the Dataset to Local DB MicrosoftSQL
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# CONFIGURATION
DB_SERVER = r"localhost\SQLEXPRESS"  # Your SQL Server instance name
DB_NAME = "datakirk"  # Target database name, I created a DB named "datakirk" already
DRIVER = "ODBC Driver 17 for SQL Server"  # This is my installed ODBC Driver, I have both 17 and 18 but 18 requires trusted authentication


# CREATING DB CONNECTION
def get_engine():
    """Create and return a SQLAlchemy engine for SQL Server using Windows Authentication."""
    connection_url = (
        f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
        f"?driver={DRIVER.replace(' ', '+')}&trusted_connection=yes"
    )
    return create_engine(connection_url)


# LOAD CSV INTO SQL SERVER
def load_csv_to_sql(table_name, file_path, engine):
    """Load a CSV file into a SQL Server table."""
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    df = pd.read_csv(file_path)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False) # I added this as safe net incase you already loaded the table before so it overwrites
    print(f"✅ Loaded '{table_name}' ({len(df)} rows) into SQL Server.")
# The above line is in case our dataset is updated which I plan to later

# MAIN PIPELINE -- This could be moved to main.py which is our main pipeline
if __name__ == "__main__":
    # Connect to the DB
    engine = get_engine()

    # Detect project root automatically by going up until "data" folder is found,
    # I had to add this another safety because at first it was difficult getting path to my data
    # Meanwhile, I don't want to hardcode the path for flexibility with team members and anyone accessing this script.
    current_path = Path(__file__).resolve()
    while not (current_path / "data" / "cleaned").exists() and current_path.parent != current_path:
        current_path = current_path.parent

    CLEANED_DIR = current_path / "data" / "cleaned"

    datasets = {
        "crude_oil": CLEANED_DIR / "crude_oil_cleaned.csv",
        "emissions": CLEANED_DIR / "uk_emissions_cleaned.csv",
        "fossil_fuel": CLEANED_DIR / "fossil_fuel_cleaned.csv"
    }

    # Load each dataset into SQL Server
    for table, file_path in datasets.items():
        load_csv_to_sql(table, file_path, engine)

    print(" All cleaned datasets loaded successfully into SQL Server!")