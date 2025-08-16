# Importing the Crude oil dataset from Kaggle using API

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi


load_dotenv()

# Configuring Kaggle API
KAGGLE_CONFIG_DIR = os.getenv("KAGGLE_CONFIG_DIR", "./config")
print(KAGGLE_CONFIG_DIR)
os.environ["KAGGLE_CONFIG_DIR"] = str(Path(KAGGLE_CONFIG_DIR).resolve())


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
    df = df[df['year'].between(1990, 2024)]

    if 'price' in df.columns:
        df.rename(columns={'price': 'price_used_per_barrel'}, inplace=True)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"✅ Crude oil data processed and saved to: {output_path}")
    return df
