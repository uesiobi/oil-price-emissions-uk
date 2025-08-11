# loading my Data into local MySQL using SQLAlchemy + Pandas:


import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:password@localhost:3306/datakirk")

# Load cleaned CSVs into database tables
pd.read_csv("C:/Users/uzo2e/Documents/DataKirk/python/DataKirk_Sprint4_Project/data/cleaned/crude_oil_cleaned.csv").to_sql("crude_oil", con=engine, if_exists="replace", index=False)
pd.read_csv("C:/Users/uzo2e/Documents/DataKirk/python/DataKirk_Sprint4_Project/data/cleaned/uk_emissions_cleaned.csv").to_sql("emissions", con=engine, if_exists="replace", index=False)
pd.read_csv("C:/Users/uzo2e/Documents/DataKirk/python/DataKirk_Sprint4_Project/data/cleaned/fossil_fuel_cleaned.csv").to_sql("fossil_fuel", con=engine, if_exists="replace", index=False)

print("All data loaded into MySQL database")