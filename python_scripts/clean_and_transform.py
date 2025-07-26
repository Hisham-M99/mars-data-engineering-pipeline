import pandas as pd
import os
import numpy as np
from datetime import datetime

STAGING_DATA_DIR = "data/staging"
PROCESSED_DATA_DIR = "data/processed"

def clean_mars_rover_photos():
    print("Cleaning Mars Rover Photos data...")
    input_filepath = os.path.join(STAGING_DATA_DIR, "mars_rover_photos", "mars_rover_photos.parquet")
    output_dir = os.path.join(PROCESSED_DATA_DIR, "mars_rover_photos")
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(input_filepath):
        print(f"Mars Rover Photos staging data not found: {input_filepath}")
        return

    df = pd.read_parquet(input_filepath)
    
    # Data cleaning and transformation
    print(f"Original Mars Rover Photos data shape: {df.shape}")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['id'])
    
    # Handle missing values
    df = df.dropna(subset=['id', 'sol', 'earth_date'])  # Drop rows with missing critical fields
    
    # Data type conversions
    df['earth_date'] = pd.to_datetime(df['earth_date'])
    df['sol'] = df['sol'].astype(int)
    df['id'] = df['id'].astype(int)
    
    # Fill missing camera names with 'Unknown'
    df['camera_name'] = df['camera_name'].fillna('Unknown')
    df['camera_short_name'] = df['camera_short_name'].fillna('Unknown')
    
    # Add derived columns
    df['year'] = df['earth_date'].dt.year
    df['month'] = df['earth_date'].dt.month
    df['day_of_week'] = df['earth_date'].dt.day_name()
    
    print(f"Cleaned Mars Rover Photos data shape: {df.shape}")
    
    # Save cleaned data
    output_filepath = os.path.join(output_dir, "cleaned_mars_rover_photos.parquet")
    df.to_parquet(output_filepath, index=False)
    print(f"Cleaned Mars Rover Photos saved to: {output_filepath}")
    
    return df

def clean_insight_weather():
    print("Cleaning Insight Weather data...")
    input_filepath = os.path.join(STAGING_DATA_DIR, "insight_weather", "insight_weather.parquet")
    output_dir = os.path.join(PROCESSED_DATA_DIR, "insight_weather")
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(input_filepath):
        print(f"Insight Weather staging data not found: {input_filepath}")
        return

    df = pd.read_parquet(input_filepath)
    
    # Data cleaning and transformation
    print(f"Original Insight Weather data shape: {df.shape}")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['sol'])
    
    # Handle missing values
    df = df.dropna(subset=['sol'])  # Drop rows with missing sol
    
    # Data type conversions
    df['sol'] = df['sol'].astype(int)
    
    # Handle missing temperature, pressure, and wind speed values
    # For numerical columns, we can either drop rows or fill with median/mean
    numerical_cols = ['avg_temp_celsius', 'min_temp_celsius', 'max_temp_celsius', 
                     'avg_pressure_pa', 'avg_wind_speed_mps']
    
    for col in numerical_cols:
        if col in df.columns:
            # Fill missing values with median
            df[col] = df[col].fillna(df[col].median())
    
    # Fill missing season with 'Unknown'
    df['season'] = df['season'].fillna('Unknown')
    
    # Convert UTC timestamps to datetime
    if 'first_utc' in df.columns:
        df['first_utc'] = pd.to_datetime(df['first_utc'], errors='coerce')
    if 'last_utc' in df.columns:
        df['last_utc'] = pd.to_datetime(df['last_utc'], errors='coerce')
    
    print(f"Cleaned Insight Weather data shape: {df.shape}")
    
    # Save cleaned data
    output_filepath = os.path.join(output_dir, "cleaned_insight_weather.parquet")
    df.to_parquet(output_filepath, index=False)
    print(f"Cleaned Insight Weather saved to: {output_filepath}")
    
    return df

def prepare_for_joining():
    print("Preparing data for joining...")
    
    # Load cleaned data
    rover_photos_path = os.path.join(PROCESSED_DATA_DIR, "mars_rover_photos", "cleaned_mars_rover_photos.parquet")
    weather_path = os.path.join(PROCESSED_DATA_DIR, "insight_weather", "cleaned_insight_weather.parquet")
    
    if not os.path.exists(rover_photos_path) or not os.path.exists(weather_path):
        print("Cleaned data files not found. Please run cleaning functions first.")
        return
    
    rover_df = pd.read_parquet(rover_photos_path)
    weather_df = pd.read_parquet(weather_path)
    
    # Create aggregated rover photos data by sol for joining
    rover_agg = rover_df.groupby(['sol', 'earth_date']).agg({
        'id': 'count',  # Count of photos per sol
        'rover_name': 'first',  # Rover name (should be consistent)
        'camera_name': lambda x: ', '.join(x.unique()),  # Unique cameras used
        'year': 'first',
        'month': 'first',
        'day_of_week': 'first'
    }).reset_index()
    
    rover_agg.rename(columns={'id': 'photo_count'}, inplace=True)
    
    # Prepare weather data for joining
    weather_df_join = weather_df[['sol', 'avg_temp_celsius', 'min_temp_celsius', 'max_temp_celsius',
                                 'avg_pressure_pa', 'avg_wind_speed_mps', 'season']].copy()
    
    # Save prepared data for joining
    join_ready_dir = os.path.join(PROCESSED_DATA_DIR, "join_ready")
    os.makedirs(join_ready_dir, exist_ok=True)
    
    rover_agg.to_parquet(os.path.join(join_ready_dir, "rover_photos_aggregated.parquet"), index=False)
    weather_df_join.to_parquet(os.path.join(join_ready_dir, "weather_for_join.parquet"), index=False)
    
    print(f"Rover photos aggregated data shape: {rover_agg.shape}")
    print(f"Weather data for join shape: {weather_df_join.shape}")
    print("Data prepared for joining and saved to join_ready directory.")

def create_data_quality_report():
    print("Creating data quality report...")
    
    report = []
    report.append("# Data Quality Report")
    report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Check rover photos data
    rover_photos_path = os.path.join(PROCESSED_DATA_DIR, "mars_rover_photos", "cleaned_mars_rover_photos.parquet")
    if os.path.exists(rover_photos_path):
        df = pd.read_parquet(rover_photos_path)
        report.append("## Mars Rover Photos Data Quality")
        report.append(f"- Total records: {len(df)}")
        report.append(f"- Date range: {df['earth_date'].min()} to {df['earth_date'].max()}")
        report.append(f"- Sol range: {df['sol'].min()} to {df['sol'].max()}")
        report.append(f"- Unique rovers: {df['rover_name'].nunique()}")
        report.append(f"- Unique cameras: {df['camera_name'].nunique()}")
        report.append("")
    
    # Check weather data
    weather_path = os.path.join(PROCESSED_DATA_DIR, "insight_weather", "cleaned_insight_weather.parquet")
    if os.path.exists(weather_path):
        df = pd.read_parquet(weather_path)
        report.append("## Insight Weather Data Quality")
        report.append(f"- Total records: {len(df)}")
        report.append(f"- Sol range: {df['sol'].min()} to {df['sol'].max()}")
        report.append(f"- Temperature range: {df['avg_temp_celsius'].min():.1f}°C to {df['avg_temp_celsius'].max():.1f}°C")
        report.append(f"- Pressure range: {df['avg_pressure_pa'].min():.1f} Pa to {df['avg_pressure_pa'].max():.1f} Pa")
        report.append("")
    
    # Save report
    report_path = os.path.join(PROCESSED_DATA_DIR, "data_quality_report.md")
    with open(report_path, 'w') as f:
        f.write('\n'.join(report))
    
    print(f"Data quality report saved to: {report_path}")

if __name__ == "__main__":
    # Ensure the output directory exists relative to the script's execution location
    script_dir = os.path.dirname(__file__)
    os.chdir(os.path.join(script_dir, '..')) # Change to data_engineering_portfolio directory
    
    # Run cleaning and transformation pipeline
    clean_mars_rover_photos()
    clean_insight_weather()
    prepare_for_joining()
    create_data_quality_report()
    
    print("Data cleaning and transformation pipeline completed successfully!")

