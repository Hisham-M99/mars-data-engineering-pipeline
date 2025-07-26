import requests
import os
import json

# Configuration
NASA_API_KEY = os.environ.get("NASA_API_KEY")
if NASA_API_KEY is None:
    raise ValueError("NASA_API_KEY environment variable not set. Please set it before running the script.")

OUTPUT_DIR = "data/raw/insight_weather"

def extract_insight_weather():
    print("Starting Insight Mars Weather data extraction")
    url = f"https://api.nasa.gov/insight_weather/?api_key={NASA_API_KEY}&feedtype=json&ver=1.0"
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        
        if data:
            output_filepath = os.path.join(OUTPUT_DIR, "insight_weather_data.json")
            os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
            with open(output_filepath, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Successfully extracted and saved Insight weather data to {output_filepath}")
        else:
            print("No weather data found")
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Insight weather data: {e}")
    except json.JSONDecodeError:
        print(f"Error decoding JSON. Response content: {response.text}")

if __name__ == "__main__":
    # Ensure the output directory exists relative to the script's execution location
    script_dir = os.path.dirname(__file__)
    os.chdir(os.path.join(script_dir, '..')) # Change to data_engineering_portfolio directory
    
    extract_insight_weather()


