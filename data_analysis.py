import os
import sys
from pathlib import Path

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import LineString, Point

from database import Database


def load_env() -> None:
    """Load environment variables.

    :return:
    """
    load_dotenv()
    if os.getenv("HOST") == "" and os.getenv("DB_USER") == "" and os.getenv("DB_PW") == "" and os.getenv("DB") == "":
        print("Environment variables are faulty. Please fix.")


def load_station_coords(file: Path = Path("station_coords.csv")) -> pd.DataFrame:
    """Load station data from file.

    :return: Returns pandas.DataFrame containing coordinates of the weather stations.
    """
    try:
        mapping = pd.read_csv(
            file,
            header="infer",
        )
        mapping["lat_station"] = pd.to_numeric(mapping["lat_station"], errors="coerce")
        mapping["lon_station"] = pd.to_numeric(mapping["lon_station"], errors="coerce")
        return mapping
    except FileNotFoundError:
        sys.exit("File not found, please run create_station_coords.py to create the station data.")


def get_nearest_id(coords: pd.DataFrame, lat_input: float, lon_input: float) -> tuple[int, float, float] | None:
    """Get the nearest ID from the database.

    :param coords: pandas.DataFrame containing coordinates from the database.
    :param lat_input: Latitude input in decimal degrees.
    :param lon_input: Longitude input in decimal degrees.
    :return: tuple containing the nearest ID and the appropriate grid coordinates.
    """
    # Calculate absolute differences between input coordinates and table coordinates
    coords["lat_diff"] = (coords["lat"] - lat_input).abs()
    coords["lon_diff"] = (coords["lon"] - lon_input).abs()

    # Find the minimum lat_diff and lon_diff
    min_lat_diff = coords["lat_diff"].min()
    min_lon_diff = coords["lon_diff"].min()

    # Filter rows where lat_diff and lon_diff are minimum respectively
    nearest_lat_rows = coords[coords["lat_diff"] == min_lat_diff]
    nearest_lon_rows = coords[coords["lon_diff"] == min_lon_diff]

    # Find intersection of these two sets (nearest in both lat and lon)
    nearest_rows = pd.merge(nearest_lat_rows, right=nearest_lon_rows, how="inner")

    if not nearest_rows.empty:
        nearest_id = (int(nearest_rows.iloc[0]["id"]), nearest_rows.iloc[0]["lat"], nearest_rows.iloc[0]["lon"])
        print(f"\nInput Coordinates:\t\t{lat_input:.2f}, {lon_input:.2f}")
        print(f"Nearest Coordinates:\t{nearest_rows.iloc[0]["lat"]:.2f}, {nearest_rows.iloc[0]["lon"]:.2f}")
        print(f"Nearest weather station:\t{nearest_rows.iloc[0]["lat_station"]:.2f}, {nearest_rows.iloc[0]["lon_station"]:.2f}")
        print(f"Nearest ID: {nearest_id[0]}\n")
        return nearest_id

    print("No nearest coordinate found.")
    return


def create_map(
    coords: pd.DataFrame,
    nearest_id: tuple[int, float, float],
    highlight_point: tuple[float, float]
) -> None:
    """Create a map with the highlighted point.
    
    Contains the highlighted point, nearest database id, the grid from the database and appropriate weather stations.

    :param nearest_id: tuple containing the database id, latitude and longitude of the nearest point in the grid.
    :param coords: pandas.DataFrame containing coordinates from the database.
    :param highlight_point: tuple containing latitude and longitude of the highlighted point in decimal degrees.
    :return: 
    """
    print("Generating Map...")
    # Convert DataFrame to GeoDataFrame with correct CRS
    gdf = gpd.GeoDataFrame(
        coords,
        geometry=gpd.points_from_xy(coords["lon"], coords["lat"]),
        crs="EPSG:4326"  # WGS84 lon/lat
    ).to_crs(epsg=3857)

    gdf_station = gpd.GeoDataFrame(
        coords,
        geometry=gpd.points_from_xy(coords["lon_station"], coords["lat_station"]),
        crs="EPSG:4326"  # WGS84 lon/lat
    ).to_crs(epsg=3857)

    # Create line geometries between each grid point and station point
    lines = [
        LineString([gdf.geometry.iloc[i], gdf_station.geometry.iloc[i]])
        for i in range(len(coords))
    ]
    gdf_lines = gpd.GeoDataFrame(geometry=lines, crs=gdf.crs)

    highlight_gdf = gpd.GeoDataFrame(
        geometry=[Point(highlight_point[1], highlight_point[0])],  # (lon, lat)
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    highlight_gdf1 = gpd.GeoDataFrame(
        geometry=[Point(nearest_id[2], nearest_id[1])],  # (lat, lon)
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    fig, ax = plt.subplots(figsize=(10, 10), dpi=250)
    gdf_lines.plot(ax=ax, color="orange", linewidth=1, label="connection to nearest weather station", zorder=1)
    gdf_station.plot(ax=ax, color="orange", markersize=10, label="weather stations", zorder=2)
    gdf.plot(ax=ax, color="blue", markersize=10, label="database grid", zorder=3)
    highlight_gdf.plot(ax=ax, color="red", markersize=80, marker="*", label=f"Given Point: ({highlight_point[0]:.2f}, {highlight_point[1]:.2f})", zorder=4)
    highlight_gdf1.plot(ax=ax, color="green", markersize=20, label=f"Nearest ID: {nearest_id[0]}", zorder=5)

    # Adjust bounds to include highlight point
    x_min, y_min, x_max, y_max = gdf.total_bounds
    hx_min, hy_min, hx_max, hy_max = highlight_gdf.total_bounds
    #hx_min, hy_min, hx_max, hy_max = highlight_gdf1.total_bounds

    x_min = min(x_min, hx_min)
    y_min = min(y_min, hy_min)
    x_max = max(x_max, hx_max)
    y_max = max(y_max, hy_max)

    # Add some padding around points (in meters)
    pad = 50000  # 10 km padding
    ax.set_xlim(x_min - pad, x_max + pad)
    ax.set_ylim(y_min - pad, y_max + pad)

    # Add basemap tiles (zoom level will adapt to axis limits)
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)

    ax.set_axis_off()
    plt.legend()
    plt.tight_layout()
    plt.show()


def create_graph(
    weatherdata: pd.DataFrame,
    id_: int, source: str,
    median_w: bool = False,
    median_m: bool = False,
    mean_w: bool = False,
    mean_m: bool = False
) -> None:
    """Create a graph visualizing the data from the dataframe for a given column.

    Optionally also display weekly and/or monthly mean and/or average.

    :param weatherdata: pandas.DataFrame containing the weather data for a specific id.
    :param median_w: Adds the weekly median to the graph
    :param median_m: Adds the monthly median to the graph
    :param mean_w: Adds the weekly mean to the graph
    :param mean_m: Adds the monthly mean to the graph
    :param id_: Database ID for reference
    :param source: Table column of the dataframe (temp, humidity, clouds, rain, wind, wind_dir, gusts)
    :return: A picture containing the graph
    """
    color_index = 0
    colors = ("red", "orange", "purple", "pink")

    match source:
        case "temp":
            title = f"Temperature Over Time (ID={id_})"
            label = f"Temperature (°C)"
        case "humidity":
            title = f"Humidity Over Time (ID={id_})"
            label = f"Humidity (%)"
        case "clouds":
            title = f"Clouds Over Time (ID={id_})"
            label = f"Cloud coverage (%)"
        case "rain":
            title = f"Rain Over Time (ID={id_})"
            label = f"Rain (mm)"
        case "wind":
            title = f"Wind Over Time (ID={id_})"
            label = f"Wind (km/h)"
        case "wind_dir":
            title = f"Wind Direction Over Time (ID={id_})"
            label = f"Wind Direction (degrees)"
        case "gusts":
            title = f"Gusts Over Time (ID={id_})"
            label = f"Gusts (km/h)"
        case _:
            print(f"Invalid source for graph: {source}")
            return
    print(f"Generating graph for {source}...")

    plt.figure(figsize=(12, 6), dpi=250)

    # Plot all original temperature points as light dots
    if source != "rain":
        plt.plot(weatherdata.index, weatherdata[source],
                 marker=".", linestyle="None", alpha=0.3, label="Original Data")
    else:
        quantile = weatherdata["rain"].quantile(.99999)
        weatherdata["rain_quantile"] = weatherdata["rain"].where(weatherdata["rain"] <= quantile)
        source = "rain_quantile"
        plt.plot(weatherdata.index, weatherdata[source],
                 marker=".", linestyle="None", alpha=0.3, label="99,999% of Original Data")

    # Plot weekly median
    if median_w:
        df_weekly_median = weatherdata.resample("W").median(numeric_only=True)
        plt.plot(df_weekly_median.index, df_weekly_median[source],
                marker="s", linestyle="--", color=colors[color_index], label="Weekly Median")
        color_index += 1

    # Plot monthly median
    if median_m:
        df_monthly_median = weatherdata.resample("M").median(numeric_only=True)
        plt.plot(df_monthly_median.index, df_monthly_median[source],
                marker="s", linestyle="--", color=colors[color_index], label="Monthly Median")
        color_index += 1

    # Plot weekly mean
    if mean_w:
        df_weekly_mean = weatherdata.resample("W").mean(numeric_only=True)
        plt.plot(df_weekly_mean.index, df_weekly_mean[source],
                marker="s", linestyle="--", color=colors[color_index], label="Weekly Mean")
        color_index += 1

    # Plot weekly mean
    if mean_m:
        df_monthly_mean = weatherdata.resample("M").mean(numeric_only=True)
        plt.plot(df_monthly_mean.index, df_monthly_mean[source],
                     marker="s", linestyle="--", color=colors[color_index], label="Monthly Mean")
        color_index += 1

    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(label)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    

def main() -> None:
    load_env()
    # create the database connection
    db = Database(os.getenv("HOST"), os.getenv("DB_USER"), os.getenv("DB_PW"), os.getenv("DB"))

    # Berlin
    lat = 52.52
    lon = 13.40

    coords = db.get_coords()

    true_coords = load_station_coords()

    coords = coords.merge(true_coords, how="right", on="id")

    nearest_id = get_nearest_id(coords, lat_input=lat, lon_input=lon)

    weatherdata = db.get_data_from_id(nearest_id[0])
    weatherdata.set_index("time", inplace=True)

    create_map(coords, nearest_id=nearest_id, highlight_point=(lat, lon))

    create_graph(weatherdata, id_=nearest_id[0], source="temp", mean_m=True)
    create_graph(weatherdata, id_=nearest_id[0], source="humidity", mean_m=True)
    create_graph(weatherdata, id_=nearest_id[0], source="clouds", mean_m=True)
    create_graph(weatherdata, id_=nearest_id[0], source="rain")
    create_graph(weatherdata, id_=nearest_id[0], source="wind", mean_m=True)
    create_graph(weatherdata, id_=nearest_id[0], source="wind_dir", mean_m=True)
    create_graph(weatherdata, id_=nearest_id[0], source="gusts", mean_m=True)

    db.close()

if __name__ == "__main__":
    main()
