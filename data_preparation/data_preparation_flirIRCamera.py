"""
This script retrieves the data from the FLIR IR camera and saves it in a folder.
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

def parse_ir_csv_file(ir_csv_path: str) -> dict:
    """
    Parses the CSV file from the FLIR IR camera and returns a dictionary.

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

            # Remove the index column
            if "index" in df.columns:
                del df["index"]
                
            # Rename date to scan_date
            df.rename(columns={"date": "scan_date"}, inplace=True)
            
            # Remove everything after last underscore in the scan_date column
            try:  
                df["sd"] = df["scan_date"].str.rsplit("_", n=1, expand=True)[0]
                df["sd"] = pd.to_datetime(df["sd"], format="%Y-%m-%d__%H-%M-%S-%f").dt.strftime('%Y%m%dT%H%M%S.%f%z')
            except ValueError:
                df["sd"] = df["scan_date"].str.split("-", n=1, expand=True)[1]
                df["sd"] = pd.to_datetime(df["sd"], format="%Y-%m-%d__%H-%M-%S-%f").dt.strftime('%Y%m%dT%H%M%S.%f%z')
            except ValueError:
                df["sd"] = pd.to_datetime(df["scan_date"], format="%Y-%m-%d__%H-%M-%S-%f").dt.strftime('%Y%m%dT%H%M%S.%f%z')
            finally:
                df["scan_date"] = df["sd"]
                df.drop(columns=["sd"], inplace=True)

            # Add UTC timezone to the scan_date using -0700
            df["scan_date"] = df["scan_date"] + "-0700"

            df["sensor"] = "flir_ir_camera"
            df["plant_name"] = df["plant_name"].fillna("NA")

            df["roi_temp"] = df["roi_temp"].fillna(0)
            
            # if the df contains genotype_x or genotype_y, then fillNA
            if "genotype_x" in df.columns:
                df["genotype_x"] = df["genotype_x"].fillna("NA")
            if "genotype_y" in df.columns:
                df["genotype_y"] = df["genotype_y"].fillna("NA")

            # Convert the dataframe to a dictionary
            data = df.to_dict(orient="records")

            # Make a data field called "loc" that contains the lat and lon
            data = [
                {**data_point, "loc": {"lat": data_point["lat"], "lon": data_point["lon"]}, "file_size": file_size, "file_path": file_path}
                for data_point in data
            ]

            # print(data[0])

            return data

def parse_url_details(url: str) -> dict:
    """
    Parses the URL to extract season, crop_type, level, and instrument information.
    Parameters:
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
    data = parse_ir_csv_file(ir_csv_path)
    url_details = parse_url_details(ir_csv_path)
    data = [dict(data_point, **url_details) for data_point in data]

    # Save the data to a file
    output_dir = "output/flir_ir_camera"
    if not path.exists(output_dir):
        os.makedirs(output_dir)

    output_path = path.join(output_dir, f"flir_ir_camera_{url_details['season']}_{url_details['crop_type']}_{url_details['level']}.json")
    with open(output_path, "w") as file:
        json.dump(data, file, indent=4)

    print(f"Data saved to {output_path}")





if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_preparation_flirIRCamera.py <ir_csv_path>")
        sys.exit(1)

    ir_csv_path = sys.argv[1]
    main(ir_csv_path)