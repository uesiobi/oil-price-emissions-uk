"""Here I imported the function from crude oil python file; the two functions;
one downloads the raw data from kaggle using API while the second do a bit of transformation"""


from etl.crude_etl import download_crude_data_kaggle, process_crude_data
# File paths for saving the data
raw_dir = "data/raw"
cleaned_crude = "data/cleaned/crude_oil_cleaned.csv"

# Step 1: Download from Kaggle
download_crude_data_kaggle(raw_dir)

# Step 2: Clean and save
process_crude_data(raw_dir, cleaned_crude)



