
import requests
import os
import json
from datetime import datetime, timedelta

# Configuration
NASA_API_KEY = os.environ.get("NASA_API_KEY")
if NASA_API_KEY is None:
    raise ValueError("NASA_API_KEY environment variable not set. Please set it before running the script.")

ROVER_NAME = "Curiosity"
# Define a date range for extraction, e.g., last 30 days from today
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=30)

OUTPUT_DIR = "data/raw/mars_rover_photos"

def extract_mars_rover_photos(start_date, end_date):
    print(f"Starting Mars Rover Photos extraction from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    current_date = start_date
    while current_date <= end_date:
        earth_date_str = current_date.strftime("%Y-%m-%d")
        url = f"https://api.nasa.gov/mars-photos/api/v1/rovers/{ROVER_NAME}/photos?earth_date={earth_date_str}&api_key={NASA_API_KEY}"
        
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            
            if data and data['photos']:
                output_filepath = os.path.join(OUTPUT_DIR, f"curiosity_photos_{earth_date_str}.json")
                os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
                with open(output_filepath, 'w') as f:
                    json.dump(data, f, indent=4)
                print(f"Successfully extracted and saved data for {earth_date_str} to {output_filepath}")
            else:
                print(f"No photos found for {earth_date_str}")
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {earth_date_str}: {e}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {earth_date_str}. Response content: {response.text}")
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    # Ensure the output directory exists relative to the script's execution location
    script_dir = os.path.dirname(__file__)
    os.chdir(os.path.join(script_dir, '..')) # Change to data_engineering_portfolio directory
    
    extract_mars_rover_photos(START_DATE, END_DATE)



