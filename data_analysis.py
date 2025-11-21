import json
import sys
import pandas as pd
from database import Database
import os
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
import contextily as ctx
from shapely.geometry import LineString

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

def load_true_coords():
    global coords
    mapping = pd.read_csv(
        "coords.txt",
        header=None,
        names=["id", "lat_station", "lon_station"],
    )

    coords = coords.merge(mapping, how="right", on="id")

def save_config() -> None:
    """Saves the config to the file specified in config_path

    :return:
    """
    with open(config_path, 'w') as f:
        json.dump(config, f, indent = 2)
        
def get_nearest_id(lat_input: float, lon_input: float):

    # Calculate absolute differences between input coordinates and table coordinates
    coords['lat_diff'] = (coords['lat'] - lat_input).abs()
    coords['lon_diff'] = (coords['lon'] - lon_input).abs()

    # Find the minimum lat_diff and lon_diff
    min_lat_diff = coords['lat_diff'].min()
    min_lon_diff = coords['lon_diff'].min()

    # Filter rows where lat_diff and lon_diff are minimum respectively
    nearest_lat_rows = coords[coords['lat_diff'] == min_lat_diff]
    nearest_lon_rows = coords[coords['lon_diff'] == min_lon_diff]

    # Find intersection of these two sets (nearest in both lat and lon)
    nearest_rows = pd.merge(nearest_lat_rows, nearest_lon_rows, how='inner')

    if not nearest_rows.empty:
        nearest_id = (int(nearest_rows.iloc[0]['id']), nearest_rows.iloc[0]['lat'], nearest_rows.iloc[0]['lon'])
        print(f"\nInput Coordinates:       {lat_input:.2f}, {lon_input:.2f}")
        print(f"Nearest Coordinates:     {nearest_rows.iloc[0]['lat']:.2f}, {nearest_rows.iloc[0]['lon']:.2f}")
        print(f"Nearest weather station: {nearest_rows.iloc[0]['lat_station']:.2f}, {nearest_rows.iloc[0]['lon_station']:.2f}")
        print(f"Nearest ID: {nearest_id[0]}\n")
        return nearest_id

    print("No nearest coordinate found.")
    return 0

def create_map(highlight_point):
    print(f'Generating Map...')
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
        geometry=[Point(id[2], id[1])],  # (lat, lon)
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    fig, ax = plt.subplots(figsize=(10, 10), dpi=250)
    gdf_lines.plot(ax=ax, color='orange', linewidth=1, label="connection to nearest weather station", zorder=1)
    gdf_station.plot(ax=ax, color='orange', markersize=10, label="weather stations", zorder=2)
    gdf.plot(ax=ax, color='blue', markersize=10, label="database grid", zorder=3)
    highlight_gdf.plot(ax=ax, color='red', markersize=80, marker='*', label=f"Given Point: ({highlight_point[0]:.2f}, {highlight_point[1]:.2f})", zorder=4)
    highlight_gdf1.plot(ax=ax, color='green', markersize=20, label=f"Nearest ID: {id[0]}", zorder=5)

    # Adjust bounds to include highlight point
    xmin, ymin, xmax, ymax = gdf.total_bounds
    hxmin, hymin, hxmax, hymax = highlight_gdf.total_bounds
    #hxmin, hymin, hxmax, hymax = highlight_gdf1.total_bounds

    xmin = min(xmin, hxmin)
    ymin = min(ymin, hymin)
    xmax = max(xmax, hxmax)
    ymax = max(ymax, hymax)

    # Add some padding around points (in meters)
    pad = 50000  # 10 km padding
    ax.set_xlim(xmin - pad, xmax + pad)
    ax.set_ylim(ymin - pad, ymax + pad)

    # Add basemap tiles (zoom level will adapt to axis limits)
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)

    ax.set_axis_off()
    plt.legend()
    plt.tight_layout()
    plt.show()

def create_tempgraph(id):
    plt.figure(figsize=(12, 6), dpi=250)

    # Plot all original temperature points as light dots
    plt.plot(weatherdata.index, weatherdata["temp"],
             marker=".", linestyle="None", alpha=0.3, label="Original Data")

    # Plot weekly median as a thick blue line
    df_weekly_median = weatherdata.resample("W").median(numeric_only=True)
    plt.plot(df_weekly_median.index, df_weekly_median["temp"],
             marker="s", linestyle="--", color="red", label="Weekly Median")

    plt.title(f"Temperature Over Time (ID={id[0]})")
    plt.xlabel("Date")
    plt.ylabel("Temperature (°C)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    load_config()
    # create the database connection
    db = Database(config["host"], config["db-user"], config["db-pw"], config["db"])

    # Berlin
    lat = 52.52
    lon = 13.40

    coords = db.get_coords()

    load_true_coords()

    id = get_nearest_id(lat, lon)

    weatherdata = db.get_data_from_id(id[0])
    weatherdata.set_index('time', inplace=True)

    create_map((lat, lon))

    create_tempgraph(id)

    db.close()