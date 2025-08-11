"""Here I imported the function from crude oil python file; the two functions;
one downloads the raw data from kaggle using API while the second do a bit of transformation"""


from etl.crude_etl import download_crude_data_kaggle, process_crude_data
from etl.emissions_etl import extract_emissions, transform_emissions
from etl.fossil_fuel_etl import extract_fossil, transform_fossil

# File paths for saving the data
raw_dir = "data/raw"
cleaned_crude = "data/cleaned/crude_oil_cleaned.csv"

# Step 1: Download from Kaggle
download_crude_data_kaggle(raw_dir)

# Step 2: Clean and save
process_crude_data(raw_dir, cleaned_crude)


# PipeLine for Air Pollutant Emission
emissions_url = "https://assets.publishing.service.gov.uk/media/67a48db5baccec3af36b3c15/long_term_trend_air_pollutants_1990_2023.csv"

raw_emissions = extract_emissions(emissions_url)
transform_emissions(raw_emissions)


# PipeLine for Fossil Fuel
raw_fossil = extract_fossil()
transform_fossil(raw_fossil)

