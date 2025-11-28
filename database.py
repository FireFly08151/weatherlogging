import math
from datetime import datetime
from typing import Any

import mysql.connector
import pandas as pd


def _convert_data_to_df(query_data: list) -> pd.DataFrame:
    """Convert query data to pandas dataframe.

    :param query_data: List of query data.
    :return: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir",
        "gusts"
    """
    df = pd.DataFrame(query_data,
                      columns=["id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir", "gusts"])
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df["rain"] = pd.to_numeric(df["rain"], errors="coerce")
    df["wind"] = pd.to_numeric(df["wind"], errors="coerce")
    df["gusts"] = pd.to_numeric(df["gusts"], errors="coerce")
    return df


def _to_none(value: Any) -> float | None:
    """Convert values that aren't float or int to None so that they can be entered into the database.

    :param value: Value to be converted.
    :return: None or the value
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


class Database:
    """Database connector for weatherlogging"""
    
    def __init__(self, host: str, user: str, pw: str, db: str) -> None:
        """Create new instance of Database.

        :param host: database host ip or "localhost"
        :param user: database user
        :param pw: password for database user
        :param db: database name
        :return: None
        """
        self.mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=pw,
            database=db
        )
        self.cursor = self.mydb.cursor()

    def add_coords(self, id_: int, lat: float, lon: float, print_debug: bool = False) -> None:
        """Add new entry to the coords-table.

        Expected types are hinted, the coordinate format is decimal degrees (Berlin would be 52.5200, 13.4050)
        
        :param id_: 
        :param print_debug: 
        :param lon: 
        :param lat: 
        """
        query = """
                INSERT INTO coords (id, lat, lon)
                VALUES (%s, %s, %s);
                """
        values = (id_, lat, lon)
        if print_debug: print(query, values)
        self.cursor.execute(query, values)
        self.mydb.commit()

    def add_coords_from_df(self, df: pd.DataFrame, print_debug: bool = False) -> None:
        """Add the contents of the dataframe to the database.

        :param df: pandas.DataFrame with columns "id", "lat", "lon"
        :param print_debug: Print the query and values for debugging purposes
        :return:
        """
        query = """
                        INSERT INTO coords (id, lat, lon)
                        VALUES (%s, %s, %s)
                        """

        values = []
        for _, row in df.iterrows():
            cleaned = (
                row["id"],
                row["lat"],
                row["lon"],
            )
            values.append(cleaned)
        if print_debug: print(query, values)
        self.cursor.executemany(query, values)
        self.mydb.commit()

    def add_data(
        self,
        id_: int,
        time: datetime,
        temp: float,
        humidity: int,
        clouds: int,
        rain: float,
        wind: float,
        wind_dir: int,
        gusts: float,
        print_debug: bool = False
    ) -> None:
        """Add new entry to the data-table.

        "nan" gets converted to "NULL" for the database.
        :param id_: foreign key id (id from coords-table)
        :param time: timestamp for this entry
        :param temp: temperature in degrees C
        :param humidity: humidity in %
        :param clouds: cloud coverage in %
        :param rain: rain in mm
        :param wind: wind speed in km/h
        :param wind_dir: wind direction in degrees
        :param gusts: gust speeds in km/h
        :param print_debug: Print the query and values for debugging purposes
        :return: 
        """
        temp = _to_none(temp)
        humidity = _to_none(humidity)
        clouds = _to_none(clouds)
        rain = _to_none(rain)
        wind = _to_none(wind)
        wind_dir = _to_none(wind_dir)
        gusts = _to_none(gusts)

        query = """
                INSERT INTO data (id, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        values = (id_, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
        if print_debug: print(query, values)
        self.cursor.execute(query, values)
        self.mydb.commit()

    def add_data_from_df(self, df: pd.DataFrame, print_debug: bool = False) -> None:
        """Add the contents of the dataframe to the database.

        :param df: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir", "gusts"
        :param print_debug: Print the query and values for debugging purposes
        :return:
        """
        query = """
                INSERT INTO data (id, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

        values = []
        for _, row in df.iterrows():
            cleaned = (
                row["id"],
                row["time"],
                _to_none(row["temp"]),
                _to_none(row["humidity"]),
                _to_none(row["clouds"]),
                _to_none(row["rain"]),
                _to_none(row["wind"]),
                _to_none(row["wind_dir"]),
                _to_none(row["gusts"]),
            )
            values.append(cleaned)
        if print_debug: print(query, values)
        self.cursor.executemany(query, values)
        self.mydb.commit()

    def get_coords(self, print_debug: bool = False) -> pd.DataFrame:
        """Read the coord ids from the database.

        :return: pandas.DataFrame with columns "id", "lat", "lon"
        """
        print(f"Getting coords...")
        query = """
                SELECT * FROM coords
                """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall(), columns=["id", "lat", "lon"])
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def get_coords_from_id(self, id_: int, print_debug: bool = False) -> pd.DataFrame:
        """Reads the coords for a specific id from the database

        :param id_:
        :param print_debug: Print the dataframe being returned for debugging purposes
        :return: pandas.DataFrame with columns "id", "lat", "lon" containing one row
        """
        print(f"Getting coords for id {id_}...")
        query = """
                SELECT * FROM coords WHERE id = %s
                """
        values = (id_,)
        self.cursor.execute(query, values)
        df = pd.DataFrame(self.cursor.fetchall(), columns=["id", "lat", "lon"])
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def get_data(self, print_debug: bool = False) -> pd.DataFrame:
        """Reads the data from the database

        :param print_debug: Print the dataframe being returned for debugging purposes
        :return: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir",
            "gusts"
        """
        print(f"Getting data... (this might take a while)")
        query = """
                    SELECT * FROM data
                    """
        self.cursor.execute(query)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def get_data_from_id(self, id_: int, print_debug: bool = False) -> pd.DataFrame:
        """Reads the data for a specific id from the database

        :param id_:
        :param print_debug: Print the dataframe being returned for debugging purposes
        :return: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir",
            "gusts"
        """
        print(f"Getting data for id {id_}...")
        query = f"SELECT * FROM data WHERE id = {id_}"
        self.cursor.execute(query)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def get_data_from_datetime(self, time: datetime, print_debug: bool = False) -> pd.DataFrame:
        """Reads the data for a specific time from the database

        :param time: Specific time you want the data from
        :param print_debug: Print the dataframe being returned for debugging purposes
        :return: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir",
            "gusts"
        """
        print(f"Getting data from {time}...")
        query = """
                SELECT * FROM data WHERE time = %s
                """
        values = (time,)
        self.cursor.execute(query, values)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def get_data_between_datetimes(self, start: datetime, end: datetime, print_debug: bool = False) -> pd.DataFrame:
        """Reads the data for a specific time period from the database

        :param start: start of the timeframe
        :param end: end of the timeframe
        :param print_debug: Print the dataframe being returned for debugging purposes
        :return: pandas.DataFrame with columns "id", "time", "temp", "humidity", "clouds", "rain", "wind", "wind_dir",
            "gusts"
        """
        print(f"Getting data between {start} and {end}...")
        query = """
                SELECT * FROM data WHERE time >= %s AND time <= %s
                """
        values = (start, end)
        self.cursor.execute(query, values)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        if print_debug: print(df)
        return df

    def close(self) -> None:
        """Closes the connection to the database"""
        self.mydb.close()
