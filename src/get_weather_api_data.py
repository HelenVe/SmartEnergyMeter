import requests
import pandas as pd
import os


OPENMETEO_API_URL = "https://api.open-meteo.com/v1/forecast"

# Coordinates for The Hague, Netherlands
LATITUDE = 52.0766
LONGITUDE = 4.2986
TIMEZONE = "Europe/Amsterdam"

parent_dir =  os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # go up 2 directories

def get_hourly_forecast(num_hours=72, past_hours=0):
    """
    Fetches hourly weather forecast data for The Hague using Open-Meteo API.
    Can also fetch some recent historical hourly data if past_hours > 0.
    Open-Meteo generally provides up to 10 days (240 hours) of forecast.
    """
    print(f"Fetching hourly weather data (forecast: {num_hours}h, past: {past_hours}h)...")

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,weather_code,wind_speed_10m,wind_direction_10m,surface_pressure,cloud_cover,is_day,shortwave_radiation",
        "temperature_unit": "fahrenheit" if os.getenv("WEATHER_UNITS") == "imperial" else "celsius",
        "wind_speed_unit": "mph" if os.getenv("WEATHER_UNITS") == "imperial" else "ms",
        "precipitation_unit": "inch" if os.getenv("WEATHER_UNITS") == "imperial" else "mm",
        "timezone": TIMEZONE,
        "forecast_hours": num_hours,
        "past_hours": past_hours
    }

    try:
        response = requests.get(OPENMETEO_API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if not data or 'hourly' not in data:
            print("No hourly data found in response.")
            return pd.DataFrame()

        df = pd.DataFrame(data['hourly'])

        if not df.empty:

            df = df.rename(columns={'time': 'timestamp'})
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            # Map weather codes to descriptions
            # Add more from https://www.meteosource.com/wiki/weather-codes
            df['weather_description'] = df['weather_code'].map({
                0: 'Clear sky',
                1: 'Mostly clear',
                2: 'Partly cloudy',
                3: 'Overcast',
                45: 'Fog',
                48: 'Depositing rime fog',
                51: 'Drizzle: Light',
                53: 'Drizzle: Moderate',
                55: 'Drizzle: Dense',
                56: 'Freezing Drizzle: Light',
                57: 'Freezing Drizzle: Dense',
                61: 'Rain: Slight',
                63: 'Rain: Moderate',
                65: 'Rain: Heavy',
                66: 'Freezing Rain: Light',
                67: 'Freezing Rain: Heavy',
                71: 'Snow fall: Slight',
                73: 'Snow fall: Moderate',
                75: 'Snow fall: Heavy',
                77: 'Snow grains',
                80: 'Rain showers: Slight',
                81: 'Rain showers: Moderate',
                82: 'Rain showers: Violent',
                85: 'Snow showers: Slight',
                86: 'Snow showers: Heavy',
                95: 'Thunderstorm: Slight or moderate',
                96: 'Thunderstorm with slight hail',
                99: 'Thunderstorm with heavy hail',

            })

            # Convert wind direction from degrees to cardinal/ordinal directions
            df['wind_direction_cardinal'] = df['wind_direction_10m'].apply(lambda d:
                                                                           'N' if (d >= 337.5 or d < 22.5) else
                                                                           'NE' if (d >= 22.5 and d < 67.5) else
                                                                           'E' if (d >= 67.5 and d < 112.5) else
                                                                           'SE' if (d >= 112.5 and d < 157.5) else
                                                                           'S' if (d >= 157.5 and d < 202.5) else
                                                                           'SW' if (d >= 202.5 and d < 247.5) else
                                                                           'W' if (d >= 247.5 and d < 292.5) else
                                                                           'NW'
                                                                           )

            df = df.sort_values('timestamp').reset_index(drop=True)
            print(f"Successfully retrieved {len(df)} hourly weather entries.")
        else:
            print("No hourly weather data found.")
        return df
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return pd.DataFrame()


def get_current_weather():
    """
    Fetches current weather data for the location.
    Extracts the first row of the hourly forecast.
    """
    print("Fetching current weather from hourly forecast...")
    hourly_df = get_hourly_forecast(num_hours=1, past_hours=0)
    if not hourly_df.empty:
        print("Successfully retrieved current weather.")
        return hourly_df.iloc[0].to_frame().T
    return pd.DataFrame()


if __name__ == "__main__":

    print("--- Fetching Current Weather ---")
    current_weather_df = get_current_weather()
    current_weather_df.to_csv(os.path.join(parent_dir, "data/raw/current_weather_data.csv"))
    if not current_weather_df.empty:
        print(current_weather_df.T)

    print("\n--- Fetching Hourly Forecast (next 72 hours, plus 24 past hours) ---")

    hourly_forecast_df = get_hourly_forecast(num_hours=72, past_hours=24)
    if not hourly_forecast_df.empty:
        print(hourly_forecast_df.head())
        print(f"Forecast from {hourly_forecast_df['timestamp'].min()} to {hourly_forecast_df['timestamp'].max()}")
        print(f"Columns available: {hourly_forecast_df.columns.tolist()}")
        hourly_forecast_df.to_csv(os.path.join(parent_dir, "data/raw/hourly_forecast_data.csv"))