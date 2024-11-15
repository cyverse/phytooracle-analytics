"""
This script extracts data from the stereoTop and saves it in a folder.
The data is then uploaded to the OpenSearch index.
"""

from os import path
import sys
import json
import os
import re
import pandas as pd
from irods.session import iRODSSession

try:
    _IRODS_ENV_FILE = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    _IRODS_ENV_FILE = path.expanduser('~/.irods/irods_environment.json')



def parse_clustering_csv_file(ir_csv_path: str)-> dict:
    """
    Parses the CSV file from the stereoTop sensor and returns a dictionary.

    Parameters:
    - ir_csv_path (str): The path to the CSV file.

    Returns:
    - dict: The parsed data.
    """

    with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
        with session.data_objects.open(ir_csv_path, 'r') as csv_file:
            # Get the size of the file
            file_size = session.data_objects.get(ir_csv_path).size
            file_path = ir_csv_path
            # Read the CSV file
            df = pd.read_csv(csv_file, sep = ",")

            # Remove the unnamed 0th column - index column
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

            # Remove the idnex column
            df = df.loc[:, ~df.columns.str.contains('^index')]

            # Rename date to scan_date
            df.rename(columns={"date": "scan_date"}, inplace=True)

            # Convert to datetime without specifying a format
            df["scan_date"] = pd.to_datetime(df["scan_date"], errors='coerce')

            # Fill in the default time for entries with missing time components
            df["scan_date"] = df["scan_date"].apply(
                lambda x: x.replace(hour=0, minute=0, second=0) if pd.notnull(x) and x.time() == pd.Timestamp.min.time() else x
            )


            # Convert scan_date to appopriate format
            df["scan_date"] = df["scan_date"].dt.strftime('%Y%m%dT%H%M%S.%f%z')

            # Add UTC timezone to the scan_date using -0700
            df["scan_date"] = df["scan_date"] + "-0700"

            df["sensor"] = "stereoTop"


            # Convert the dataframe to a dictionary
            data = df.to_dict(orient="records")

            # Make a data field called "loc" that contains the lat and lon
            data = [
                {**data_point, "loc": {"lat": data_point["lat"], "lon": data_point["lon"]}, "file_size": file_size, "file_path": file_path}
                for data_point in data
            ]

    return data


def parse_url_details(url: str) -> dict:
    """
    Parses the URL to extract season, crop_type, level, and instrument information.
    - url (str): The tar file's URL to parse.

    Returns:
    - dict: A dictionary containing the extracted details.
    """
    pattern = (
        r"/season_([0-9]+)_([a-zA-Z]+)_yr_([0-9]+)" +
        r"/level_([0-9]+)" +
        r"/([^/]+)" +
        r"/[^w]+?"
        )
    match = re.search(pattern, url)
    if match:
        season, crop_type, year, level, instrument = match.groups()
        return {
            "season": int(season),
            "crop_type": crop_type,
            "year": int(year),
            "level": int(level),
            "instrument": instrument
        }
    # Raise an error if the URL doesn't match the expected pattern
    raise RuntimeError("Failed to parse URL. Exiting!!")


def main(ir_csv_path: str) -> None:
    """
    The main function of the script.

    Parameters:
    - ir_csv_path (str): The path to the CSV file from the FLIR IR camera.
    """

    # Parse the CSV file
    data = parse_clustering_csv_file(ir_csv_path)
    url_details = parse_url_details(ir_csv_path)
    data = [dict(data_point, **url_details) for data_point in data]

    # Save the data to a file
    output_dir = "output/stereoTop"
    if not path.exists(output_dir):
        os.makedirs(output_dir)

    output_path = path.join(output_dir, f"stereoTop.json_{url_details['season']}_{url_details['crop_type']}_{url_details['level']}.json")
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

    print(f"Data saved to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_preparation_stereoTop.py <path_to_csv>")
        sys.exit(1)

    ir_csv_path = sys.argv[1]
    main(ir_csv_path)

# /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/stereoTop/season_14_clustering.csv
# /iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_3/stereoTop/season_11_clustering.csv
