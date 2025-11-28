import os
import traceback
from datetime import datetime

import requests
from dotenv import load_dotenv

from database import Database


def load_env() -> None:
    """Load environment variables.

    :return:
    """
    load_dotenv()
    if os.getenv("HOST") == "" and os.getenv("DB_USER") == "" and os.getenv("DB_PW") == "" and os.getenv("DB") == "":
        print("Environment variables are faulty. Please fix.")


def get_data_from_api(lat: float, lon: float, print_debug: bool = False) -> dict[str, float | int]:
    """Fetches current weather data from the WeatherAPI for the given latitude and longitude.

    The function retrieves temperature, humidity, cloud cover, precipitation, wind speed,
    wind direction, and wind gusts. If any of these values are missing in the API response,
    they are returned as None.

    :param lat: Latitude of the location in decimal degrees.
    :param lon: Longitude of the location in decimal degrees.
    :param print_debug: If True, prints the full API response and extracted weather data for debugging purposes.
    :return: A dict containing:
        - temp : Temperature in Celsius.
        - humidity: Humidity percentage.
        - clouds: Cloud cover percentage.
        - rain: Precipitation in millimeters.
        - wind: Wind speed in km/h.
        - wind_dir: Wind direction in degrees.
        - gusts: Wind gusts in km/h.
    """
    url = f"https://api.weatherapi.com/v1/current.json?key={os.getenv('API_KEY')}&q={lat} {lon}&aqi=no"
    res = requests.get(url).json()
    if not isinstance(res, dict):
        return {}
    if print_debug: print(res)
    dict_ = {
        "temp": res.get("current", {}).get("temp_c"),
        "humidity": res.get("current", {}).get("humidity"),
        "clouds": res.get("current", {}).get("cloud"),
        "rain": res.get("current",{}).get("precip_mm"),
        "wind": res.get("current", {}).get("wind_kph"),
        "wind_dir": res.get("current", {}).get("wind_degree"),
        "gusts": res.get("current", {}).get("gust_kph"),
    }
    if print_debug: print(dict_)
    return dict_


def main() -> None:
    load_env()
    # create the database connection
    db = Database(os.getenv("HOST"), os.getenv("DB_USER"), os.getenv("DB_PW"), os.getenv("DB"))
    # get the coordinates from the database
    coords = db.get_coords()

    # Get the current time for the database new entries
    now = datetime.now()
    time = datetime(now.year, now.month, now.day, now.hour)

    # iterate through the coordinates and add the data to the database
    print("Getting the data from the api and adding it to the database. This might take several minutes...")
    for _, row in coords.iterrows():
        try:
            x = get_data_from_api(row["lat"], row["lon"], print_debug=False)
            db.add_data(
                int(row["id"]),
                time=time,
                temp=x["temp"],
                humidity=x["humidity"],
                clouds=x["clouds"],
                rain=x["rain"],
                wind=x["wind"],
                wind_dir=x["wind_dir"],
                gusts=x["gusts"],
                print_debug=False,
            )
        except Exception as error:
            print(f"Error processing id={int(row['id'])} lat={row['lat']} lon={row['lon']}: {type(error)}: {error}")
            traceback.print_exc()

    # close the database connection
    db.close()


if __name__ == "__main__":
    main()
