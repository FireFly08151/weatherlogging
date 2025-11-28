import os

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


def main() -> None:
    load_env()
    # create the database connection
    db = Database(os.getenv("HOST"), os.getenv("DB_USER"), os.getenv("DB_PW"), os.getenv("DB"))
    # get the coordinates from the database
    coords = db.get_coords(True)

    print("Be patient, this might take several minutes...")

    with open("station_coords.csv", "w") as f:
        f.write("id,lat_station,lon_station\n")
        for _, row in coords.iterrows():
            url = f"""
                http://api.weatherapi.com/v1/current.json?key={os.getenv("API_KEY")}
                &q={row["lat"]} {row["lon"]}&aqi=no
            """
            res = requests.get(url)
            data = res.json()
            lat = data.get("location", {}).get("lat")
            lon = data.get("location", {}).get("lon")
            if lat is None or lon is None:
                print(f"{int(row["id"])} failed: {data.get("location")}")
            else:
                f.write(f"{int(row["id"])},{lat},{lon}\n")
                print(f"{int(row['id'])}, {lat}, {lon}")


if __name__ == '__main__':
    main()
