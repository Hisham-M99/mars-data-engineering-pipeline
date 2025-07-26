
import pandas as pd
import os
import json

RAW_DATA_DIR = "data/raw"
STAGING_DATA_DIR = "data/staging"

def convert_mars_rover_photos_to_parquet():
    print("Converting Mars Rover Photos JSON to Parquet...")
    input_dir = os.path.join(RAW_DATA_DIR, "mars_rover_photos")
    output_dir = os.path.join(STAGING_DATA_DIR, "mars_rover_photos")
    os.makedirs(output_dir, exist_ok=True)

    all_photos_data = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(input_dir, filename)
            with open(filepath, "r") as f:
                data = json.load(f)
                for photo in data.get("photos", []):
                    all_photos_data.append({
                        "id": photo.get("id"),
                        "sol": photo.get("sol"),
                        "earth_date": photo.get("earth_date"),
                        "img_src": photo.get("img_src"),
                        "rover_name": photo.get("rover", {}).get("name"),
                        "camera_name": photo.get("camera", {}).get("full_name"),
                        "camera_short_name": photo.get("camera", {}).get("name")
                    })
    
    if all_photos_data:
        df = pd.DataFrame(all_photos_data)
        output_filepath = os.path.join(output_dir, "mars_rover_photos.parquet")
        df.to_parquet(output_filepath, index=False)
        print(f"Successfully converted Mars Rover Photos to Parquet: {output_filepath}")
    else:
        print("No Mars Rover Photos data to convert.")

def convert_insight_weather_to_parquet():
    print("Converting Insight Weather JSON to Parquet...")
    input_filepath = os.path.join(RAW_DATA_DIR, "insight_weather", "insight_weather_data.json")
    output_dir = os.path.join(STAGING_DATA_DIR, "insight_weather")
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(input_filepath):
        print(f"Insight weather data file not found: {input_filepath}")
        return

    with open(input_filepath, "r") as f:
        data = json.load(f)

    weather_data = []
    for sol_key in data.get("sol_keys", []):
        sol_data = data.get(sol_key, {})
        weather_data.append({
            "sol": int(sol_key),
            "avg_temp_celsius": sol_data.get("AT", {}).get("av"),
            "min_temp_celsius": sol_data.get("AT", {}).get("mn"),
            "max_temp_celsius": sol_data.get("AT", {}).get("mx"),
            "avg_pressure_pa": sol_data.get("PRE", {}).get("av"),
            "avg_wind_speed_mps": sol_data.get("HWS", {}).get("av"),
            "season": sol_data.get("Season"),
            "first_utc": sol_data.get("First_UTC"),
            "last_utc": sol_data.get("Last_UTC")
        })

    if weather_data:
        df = pd.DataFrame(weather_data)
        output_filepath = os.path.join(output_dir, "insight_weather.parquet")
        df.to_parquet(output_filepath, index=False)
        print(f"Successfully converted Insight Weather to Parquet: {output_filepath}")
    else:
        print("No Insight Weather data to convert.")

if __name__ == "__main__":
    # Ensure the output directory exists relative to the script's execution location
    script_dir = os.path.dirname(__file__)
    os.chdir(os.path.join(script_dir, '..')) # Change to data_engineering_portfolio directory
    
    convert_mars_rover_photos_to_parquet()
    convert_insight_weather_to_parquet()


