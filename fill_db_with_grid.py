import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from database import Database


def load_env() -> None:
    """Load environment variables.

    :return:
    """
    load_dotenv()
    if os.getenv("HOST") == "" and os.getenv("DB_USER") == "" and os.getenv("DB_PW") == "" and os.getenv("DB") == "":
        print("Environment variables are faulty. Please fix.")


def load_grid(file: Path = Path("grid.csv")) -> pd.DataFrame:
    """Load grid data from file.

    :return: Returns pandas.DataFrame with columns "id", "lat" and "lon".
    """
    try:
        mapping = pd.read_csv(
            file,
            header="infer",
        )
        mapping["lat"] = pd.to_numeric(mapping["lat"], errors="coerce")
        mapping["lon"] = pd.to_numeric(mapping["lon"], errors="coerce")
        for _, row in mapping.iterrows():
            mapping["id"] = mapping.index
        mapping = mapping.iloc[:, [2, 0, 1]]
        print(mapping)
        print(mapping.dtypes)
        return mapping
    except FileNotFoundError:
        sys.exit("File not found, please create grid.csv with coordinates in decimal degrees.")


def main() -> None:
    load_env()
    # create the database connection
    db = Database(os.getenv("HOST"), os.getenv("DB_USER"), os.getenv("DB_PW"), os.getenv("DB"))

    grid = load_grid()

    db.add_coords_from_df(grid, print_debug=True)

    db.close()


if __name__ == "__main__":
    main()