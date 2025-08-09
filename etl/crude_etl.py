# Importing the Crude oil dataset from Kaggle using API

import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from pathlib import Path

# Setting up kaggle API DIR; use this when you don't have the JSON file in your directory
# os.environ['KAGGLE_USERNAME']= 'brillianthub'
# os.environ['KAGGLE_KEY']= "f697215f527f47d233a43d85d97799d2"

# OR use this when you save the JSON file to a specific directory and link the path as I did
os.environ['KAGGLE_CONFIG_DIR'] = r"C:/Users/uzo2e/Documents/DataKirk/python/DataKirk_Sprint4_Project/"


# Now to download crude oil price dataset from kaggle using API
def download_crude_data_kaggle(download_path: str):
    api = KaggleApi()
    api.authenticate()

    crude_data = 'sc231997/crude-oil-price'
    api.dataset_download_files(crude_data, path=download_path, unzip=True)
    print(f" Crude oil dataset downloaded to {download_path}")


def process_crude_data(input_path: str, output_path: str) -> pd.DataFrame:
    crude_file = os.path.join(input_path, 'crude-oil-price.csv')
    df = pd.read_csv(crude_file)

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df = df[df['year'].between(2015, 2024)]

    if 'price' in df.columns:
        df.rename(columns={'price': 'price_used_per_barrel'}, inplace=True)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"✅ Crude oil data processed and saved to: {output_path}")
    return df
