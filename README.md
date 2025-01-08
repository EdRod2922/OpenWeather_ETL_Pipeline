# ETL Data Pipeline for Weather Data

This Python script is a simple ETL pipeline that extracts weather data from the OpenWeather API, transforms it into a simple format, and saves it as a CSV file.

## How It Works
1. **Extract**: Fetches weather data for a city using the OpenWeather API and API Key.
2. **Transform**: Converts the API response into a readable format (city, temperature, description).
3. **Load**: Saves the transformed data to `weather_data.csv`.
