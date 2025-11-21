import math
import mysql.connector
from datetime import datetime
import pandas as pd


def _convert_data_to_df(query_data: list) -> pd.DataFrame:
    """Converts query data to pandas dataframe

    :param query_data:
    :return: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
    """
    df = pd.DataFrame(query_data,
                      columns=['id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'])
    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    df['rain'] = pd.to_numeric(df['rain'], errors='coerce')
    df['wind'] = pd.to_numeric(df['wind'], errors='coerce')
    df['gusts'] = pd.to_numeric(df['gusts'], errors='coerce')
    return df

def _to_null(value):
    """Converts values that aren't float or int to None so that they can be entered into the database

    :param value:
    :return:
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

class Database:
    """Database connector for weatherlogging"""
    def __init__(self, host: str,user: str, pw: str, db: str):
        """Creates a new instance of Database

        :param host: database host ip or 'localhost'
        :param user: database user
        :param pw: password for database user
        :param db: database name
        """
        self.mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=pw,
            database=db
        )
        self.cursor = self.mydb.cursor()

    def add_coords(self, id: int, lat: float, lon: float) -> None:
        """Adds a new entry to the coords-table

        Expected types are hinted, the coordinate format is decimal degrees (Berlin would be 52.5200, 13.4050)
        """
        query = """
                INSERT INTO coords (id, lat, lon)
                VALUES (%s, %s, %s);
                """
        values = (id, lat, lon)
        print(query, values)
        self.cursor.execute(query, values)
        self.mydb.commit()

    def add_data(self, id: int, time: datetime, temp: float, humidity: int, clouds: int, rain: float, wind: float, wind_dir: int, gusts: float) -> None:
        """Adds a new entry to the data-table

        Expected types are hinted, additionally 'nan' gets converted to 'NULL' for the database.
        :param id: foreign key id (id from coords-table)
        :param time: timestamp for this entry
        :param temp: 
        :param humidity: 
        :param clouds: 
        :param rain: 
        :param wind: 
        :param wind_dir: 
        :param gusts: 
        :return: 
        """
        temp = _to_null(temp)
        humidity = _to_null(humidity)
        clouds = _to_null(clouds)
        rain = _to_null(rain)
        wind = _to_null(wind)
        wind_dir = _to_null(wind_dir)
        gusts = _to_null(gusts)

        query = """
                INSERT INTO data (id, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        values = (id, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
        self.cursor.execute(query, values)
        self.mydb.commit()

    def add_data_from_df(self, df: pd.DataFrame) -> None:
        """Adds the contents of the dataframe to the database

        :param df: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
        :return:
        """
        query = """
                INSERT INTO data (id, time, temp, humidity, clouds, rain, wind, wind_dir, gusts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        cols = ['id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts']

        values = []
        for _, row in df[cols].iterrows():
            cleaned = (
                row['id'],
                row['time'],
                _to_null(row['temp']),
                _to_null(row['humidity']),
                _to_null(row['clouds']),
                _to_null(row['rain']),
                _to_null(row['wind']),
                _to_null(row['wind_dir']),
                _to_null(row['gusts']),
            )
            values.append(cleaned)
        self.cursor.executemany(query, values)
        self.mydb.commit()

    def get_coords(self) -> pd.DataFrame:
        """Reads the coord ids from the database

        :return: pandas.DataFrame with columns 'id', 'lat', 'lon'
        """
        print(f'Getting coords...')
        query = """
                SELECT * FROM coords
                """
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall(), columns=['id', 'lat', 'lon'])
        self.mydb.commit()
        print(df)
        return df

    def get_coords_from_id(self, id: int) -> pd.DataFrame:
        """Reads the coords for a specific id from the database

        :param id:
        :return: pandas.DataFrame with columns 'id', 'lat', 'lon' containing one row
        """
        print(f'Getting coords for {id}...')
        query = """
                SELECT * FROM coords WHERE id = %s
                """
        values = (id,)
        self.cursor.execute(query, values)
        df = pd.DataFrame(self.cursor.fetchall(), columns=['id', 'lat', 'lon'])
        self.mydb.commit()
        print(df)
        return df

    def get_data(self) -> pd.DataFrame:
        """Reads the data from the database

        :return: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
        """
        print(f'Getting data... (this might take a while)')
        query = """
                    SELECT * FROM data
                    """
        self.cursor.execute(query)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        print(df)
        return df

    def get_data_from_id(self, id: int) -> pd.DataFrame:
        """Reads the data for a specific id from the database

        :param id:
        :return: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
        """
        print(f'Getting data from {id}...')
        query = """
                SELECT * FROM data WHERE id = %s
                """
        values = (id,)
        self.cursor.execute(query, values)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        print(df)
        return df

    def get_data_from_datetime(self, time: datetime) -> pd.DataFrame:
        """Reads the data for a specific time from the database

        :param time: Specific time you want the data from
        :return: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
        """
        print(f'Getting data from {time}...')
        query = """
                SELECT * FROM data WHERE time = %s
                """
        values = (time,)
        self.cursor.execute(query, values)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        print(df)
        return df

    def get_data_between_datetimes(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Reads the data for a specific time period from the database

        :param start: start of the timeframe
        :param end: end of the timeframe
        :return: pandas.DataFrame with columns 'id', 'time', 'temp', 'humidity', 'clouds', 'rain', 'wind', 'wind_dir', 'gusts'
        """
        print(f'Getting data between {start} and {end}...')
        query = """
                SELECT * FROM data WHERE time >= %s AND time <= %s
                """
        values = (start, end)
        self.cursor.execute(query, values)
        df = _convert_data_to_df(self.cursor.fetchall())
        self.mydb.commit()
        print(df)
        return df

    def close(self) -> None:
        """Closes the connection to the database"""
        self.mydb.close()