import os
import socket
import time
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
    if os.getenv("HOST") == "" or os.getenv("DB_USER") == "" or os.getenv("DB_PW") == "" or os.getenv("DB") == "":
        print("Environment variables are faulty. Please fix.")


def get_data_from_api(ip: str, lat: float, lon: float, print_debug: bool = False) -> dict[str, float | int]:
    """Fetches current weather data from the WeatherAPI for the given latitude and longitude.

    The function retrieves temperature, humidity, cloud cover, precipitation, wind speed,
    wind direction, and wind gusts. If any of these values are missing in the API response,
    they are returned as None.

    :param ip: IP of api.weatherapi.com
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
    url = f"https://{ip}/v1/current.json?key={os.getenv('API_KEY')}&q={lat},{lon}&aqi=no"
    headers = {"Host": "api.weatherapi.com"}
    res = requests.get(url, headers=headers).json()
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
    if all(value is None for value in dict_.values()):
        return {}
    return dict_


def main() -> tuple[int, int, float, float]:
    """ Main function for weather logging process.
    :return: A tuple containing the amount of inserted data points, total data points, time spent on api-requests and time spend on db-inserts
    """
    time_main = time.perf_counter()
    load_env()

    ip = socket.gethostbyname("api.weatherapi.com")
    print(ip)

    # create the database connection
    db: Database = Database(os.getenv("HOST"), os.getenv("DB_USER"), os.getenv("DB_PW"), os.getenv("DB"))
    # get the coordinates from the database
    coords = db.get_coords(True)

    # Get the current time for the database new entries
    now: datetime = datetime.now()
    time_acquisition: datetime = datetime(now.year, now.month, now.day, now.hour)

    time_api: float = 0
    time_db: float = 0

    counter_inserted: int = 0

    # iterate through the coordinates and add the data to the database
    print("Getting the data from the api and adding it to the database. This might take several minutes...")
    for _, row in coords.iterrows():
        try:
            start = time.perf_counter()
            x = get_data_from_api(ip, row["lat"], row["lon"], print_debug=False)
            if x == {}:
                print(f"id={int(row['id'])} lat={row['lat']} lon={row['lon']} returned empty dict")
                continue
            mid = time.perf_counter()
            db.add_data(
                int(row["id"]),
                time=time_acquisition,
                temp=x["temp"],
                humidity=x["humidity"],
                clouds=x["clouds"],
                rain=x["rain"],
                wind=x["wind"],
                wind_dir=x["wind_dir"],
                gusts=x["gusts"],
                print_debug=False,
            )
            end = time.perf_counter()
            time_api += mid - start
            time_db += end - mid
            counter_inserted += 1
            time.sleep(.5)
        except Exception as error:
            print(f"Error processing id={int(row['id'])} lat={row['lat']} lon={row['lon']}: {type(error)}: {error}")
            traceback.print_exc()

    # close the database connection
    db.close()

    print(f"Total time of API-calls: {time.strftime('%Mmin %Ss', time.gmtime(time_api))}")
    print(f"Total time of DB-calls: {time.strftime('%Mmin %Ss', time.gmtime(time_db))}")
    print(f"Inserted {counter_inserted}/{len(coords.index)} data points")
    print(f"Total Time: {time.strftime('%Mmin %Ss', time.gmtime(time.perf_counter() - time_main))}")

    return counter_inserted, len(coords.index), time_api, time_db

if __name__ == "__main__":
    main()
