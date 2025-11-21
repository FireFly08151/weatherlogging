from database import Database
from datetime import datetime
import requests
import json
import os
import sys
import traceback

script_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_dir, "config.json")
config_default = {
    "API-key": "1234abcd",
    "host": "localhost",
    "db-user": "user",
    "db-pw": "1234",
    "db": "database"}
config = config_default


def load_config(print_config = False) -> None:
    """Loads the config file specified in config_path and validates it.

    If the file is missing, corrupted, or still contains placeholder values,
    writes default config and exits to force user editing.
    """
    global config
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("ERROR: Config file missing or unreadable.")
        print("A new config.json with placeholder values has been created.")
        config = config_default.copy()
        save_config()
        sys.exit(1)
    missing_or_wrong = False

    for key, default_value in config_default.items():
        # Missing key
        if key not in config:
            print(f"ERROR: Missing config key '{key}'.")
            missing_or_wrong = True
            continue

        # Wrong type
        if not isinstance(config[key], type(default_value)):
            print(f"ERROR: Invalid type for '{key}'. Expected {type(default_value).__name__}.")
            missing_or_wrong = True

    if missing_or_wrong:
        print("\nOne or more config values were invalid.")
        print("The config file has been reset to placeholder defaults.")
        config = config_default.copy()
        save_config()
        sys.exit(1)

    if config == config_default:
        print("ERROR: Config contains only placeholder values.")
        print("Please update config.json with your real API key and database credentials.")
        sys.exit(1)

    if print_config:
        print(config)


def save_config() -> None:
    """Saves the config to the file specified in config_path

    :return:
    """
    with open(config_path, 'w') as f:
        json.dump(config, f, indent = 2)


def get_data_from_api(lat: float, lon: float, print_debug = False) -> tuple:
    """Fetches current weather data from the WeatherAPI for the given latitude and longitude.

    The function retrieves temperature, humidity, cloud cover, precipitation, wind speed,
    wind direction, and wind gusts. If any of these values are missing in the API response,
    they are returned as the string 'NULL'.

    :param lat: Latitude of the location in decimal degrees.
    :param lon: Longitude of the location in decimal degrees.
    :param print_debug: If True, prints the full API response and extracted weather data for debugging purposes.
    :return: A tuple containing:
        - temp (float or 'NULL'): Temperature in Celsius.
        - humidity (int or 'NULL'): Humidity percentage.
        - clouds (int or 'NULL'): Cloud cover percentage.
        - rain (float or 'NULL'): Precipitation in millimeters.
        - wind (float or 'NULL'): Wind speed in km/h.
        - wind_dir (int or 'NULL'): Wind direction in degrees.
        - gusts (float or 'NULL'): Wind gusts in km/h.
    """
    url = f'https://api.weatherapi.com/v1/current.json?key={config["API-key"]}&q={lat} {lon}&aqi=no'
    res = requests.get(url).json()
    if print_debug: print(res)
    try: temp = res['current']['temp_c']
    except KeyError: temp = 'NULL'
    try: humidity = res['current']['humidity']
    except KeyError: humidity = 'NULL'
    try: clouds = res['current']['cloud']
    except KeyError: clouds = 'NULL'
    try: rain = res['current']['precip_mm']
    except KeyError: rain = 'NULL'
    try: wind = res['current']['wind_kph']
    except KeyError: wind = 'NULL'
    try: wind_dir = res['current']['wind_degree']
    except KeyError: wind_dir = 'NULL'
    try: gusts = res['current']['gust_kph']
    except KeyError: gusts = 'NULL'
    if print_debug: print(temp, humidity, clouds, rain, wind, wind_dir, gusts)
    return temp, humidity, clouds, rain, wind, wind_dir, gusts

if __name__ == '__main__':
    load_config()
    # create the database connection
    db = Database(config["host"], config["db-user"], config["db-pw"], config["db"])
    # get the coordinates from the database
    coords = db.get_coords()

    # Get the current time for the database new entries
    now = datetime.now()
    time = datetime(now.year, now.month, now.day, now.hour)

    # iterate through the coordinates and add the data to the database
    print(f'Getting the data from the api and adding it to the database. This might take several minutes...')
    for _, row in coords.iterrows():
        try:
            x = get_data_from_api(row['lat'], row['lon'])
            db.add_data(row['id'], time, x[0], x[1], x[2], x[3], x[4], x[5], x[6])
        except Exception as error:
            print(f"Error processing id={row['id']} lat={row['lat']} lon={row['lon']}: {type(error)}: {error}")
            traceback.print_exc()

    # close the database connection
    db.close()
