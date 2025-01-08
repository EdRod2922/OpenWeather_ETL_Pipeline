#ETL data pipeline for API weather data

"""
Use Python to create a simple ETL data pipeline 
to extract, transform, and load weather data 
from a REST API to save as a CSV file

"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load sensitive credentials from .env file
load_dotenv()
api_key = os.getenv("API_KEY")

if not api_key:
    raise Exception("API_KEY not found. Please check your .env file.")

# Define the base URL for OpenWeather API
base_url = "https://api.openweathermap.org/data/2.5/weather"

# 1. Extract
def extract_data(city):
    url = f"{base_url}?q={city}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed: {response.status_code}, {response.text}")

# 2. Transform
def transform_data(data):
    try:
        transformed_data = {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"]
        }
        return transformed_data
    except KeyError as e:
        raise Exception(f"Unexpected response format: {e}")

# 3. Load
def load_data(data, filename):
    df = pd.DataFrame([data])
    df.to_csv(filename, index=False)
    print(f"File saved to: {os.path.abspath(filename)}")

# Run the data pipeline
def run_etl_pipeline(city):
    print("Extracting data...")
    data = extract_data(city)
    print(f"Extracted data: {data}")

    print("Transforming data...")
    transformed_data = transform_data(data)
    print(f"Transformed data: {transformed_data}")

    print("Loading data...")
    load_data(transformed_data, "weather_data.csv")
    print("Data successfully saved to CSV.")

# Specify the city
city = "Cairo"
try:
    run_etl_pipeline(city)
except Exception as e:
    print(f"Error running ETL pipeline: {e}")
