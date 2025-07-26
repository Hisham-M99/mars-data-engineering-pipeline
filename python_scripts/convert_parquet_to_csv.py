import pandas as pd
import os

PROCESSED_DATA_DIR = "data/processed/join_ready"

def convert_parquet_to_csv():
    print("Converting Parquet files to CSV...")

    # File 1: rover_photos_aggregated.parquet
    parquet_file1 = os.path.join(PROCESSED_DATA_DIR, "rover_photos_aggregated.parquet")
    csv_file1 = os.path.join(PROCESSED_DATA_DIR, "rover_photos_aggregated.csv")

    if os.path.exists(parquet_file1):
        df1 = pd.read_parquet(parquet_file1)
        df1.to_csv(csv_file1, index=False)
        print(f"Successfully converted {parquet_file1} to {csv_file1}")
    else:
        print(f"Parquet file not found: {parquet_file1}")

    # File 2: weather_for_join.parquet
    parquet_file2 = os.path.join(PROCESSED_DATA_DIR, "weather_for_join.parquet")
    csv_file2 = os.path.join(PROCESSED_DATA_DIR, "weather_for_join.csv")

    if os.path.exists(parquet_file2):
        df2 = pd.read_parquet(parquet_file2)
        df2.to_csv(csv_file2, index=False)
        print(f"Successfully converted {parquet_file2} to {csv_file2}")
    else:
        print(f"Parquet file not found: {parquet_file2}")

if __name__ == "__main__":
    # Ensure the script runs from the data_engineering_portfolio directory
    script_dir = os.path.dirname(__file__)
    os.chdir(os.path.join(script_dir, '..'))
    convert_parquet_to_csv()

