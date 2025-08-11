import pandas as pd
import requests
from pathlib import Path


def extract_fossil(country_code="GB", indicator="EG.USE.COMM.FO.ZS", raw_dir="data/raw"):
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(raw_dir) / f"{country_code}_fossil_fuel.csv"

    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}?format=json&per_page=500"
    r = requests.get(url)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch fossil fuel data. Status: {r.status_code}")

    data = r.json()
    if not data or len(data) < 2:
        raise ValueError("Unexpected API format or no data returned.")

    df = pd.DataFrame(data[1])
    df.to_csv(file_path, index=False)
    print(f"Fossil fuel data saved to {file_path}")
    return file_path


def transform_fossil(file_path, output_path="data/cleaned/fossil_fuel_cleaned.csv"):
    df = pd.read_csv(file_path)

    # Keeping only relevant columns
    df = df[['date', 'value']].rename(columns={'date': 'year', 'value': 'fossil_pct'})
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df = df[(df['year'] >= 2015) & (df['year'] <= 2024)]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Fossil fuel data cleaned and saved to {output_path}")
    return df
