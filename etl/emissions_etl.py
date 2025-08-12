import pandas as pd
import requests
from pathlib import Path


def extract_emissions(url, raw_dir = "data/raw", filename = "uk_emissions.csv"):
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(raw_dir)/filename

    r = requests.get(url)
    if r.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(r.content)
        print(f" Emissions data downloaded to {file_path}")
    else:
        raise RuntimeError(f"Failed to download Emission data. Status: {r.status_code}")

    return file_path


def transform_emissions(file_path,output_path = "data/cleaned/uk_emissions_cleaned.csv"):
    df = pd.read_csv(file_path)

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Rename the emission column
    if 'emissions_(indexed_to_1990)' in df.columns:
        df.rename(columns={'emissions_(indexed_to_1990)': 'emissions'}, inplace=True)

    if 'year' in df.columns:
        df = df[df['year'].between(2015, 2024)]
        df['year'] = pd.to_numeric(df['year'], errors='coerce')  # convert to number

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Emissions data cleaned and saved to {output_path}")
    return df