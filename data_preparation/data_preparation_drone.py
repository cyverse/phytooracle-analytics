"""
This script extracts data from the drone and saves it in a folder.
The data is then uploaded to the OpenSearch index.
"""

from os import path
import sys
import json
import os
import re
import tarfile
import pandas as pd
from dateutil.parser import parse
from irods.session import iRODSSession

try:
    _IRODS_ENV_FILE = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    _IRODS_ENV_FILE = path.expanduser('~/.irods/irods_environment.json')



def extract_csv_from_tar_file(irods_file_path: str) -> pd.DataFrame:
    """
    Downloads the tar file from iRODS and extracts the CSV file in:
    - <extracted_folder>/tgi_extraction_out/<filename>.csv

    Parameters:
    - irods_file_path (str): The path to the tar file in iRODS.

    Returns:
    - csv_file: the data in the CSV file.
    """

    try:
        # Access the file using iRODS
        with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
            with session.data_objects.open(irods_file_path, 'r') as tar_file:
                assert tarfile.is_tarfile(tar_file), "The file is not a tar file."
                # Extract the tar file into a temporary folder
                with tarfile.open(fileobj=tar_file, mode='r') as tar:
                    for member in tar.getmembers():
                        if member.isfile() and member.name.endswith('.csv'):
                            csv_file = tar.extractfile(member)
                            df = pd.read_csv(csv_file)
                            # print(df.head())
                            df["file_path"] = member.name
                            df["file_size"] = member.size
                            # print(df.head())
                            print(f"Extracted from {irods_file_path}")
                            print(df.head())
                            return df

    except FileNotFoundError as fe:
        print(f"File not found: {fe}")
        return pd.DataFrame()
    except pd.errors.EmptyDataError as ede:
        print(f"The file is empty or invalid.")
        return pd.DataFrame()
    except ValueError as ve:
        print(f"Data conversion issue: {ve}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def get_output(df: pd.DataFrame, irods_file_path: str) -> list:
    """
    Saves the output as JSON to output/drone/

    Parameters:
    - df (pd.DataFrame): The data to save.
    - irods_file_path (str): The path to the tar file in iRODS.
    """

    # Extract relevant data from the file path
    pattern = pattern = r"season_(\d+)_(\w+)_yr_(\d+)/level_(\d+)/(\w+)/\w+/(\d{4}-\d{2}-\d{2})_Gantry_(North|South)_(P\d)_(\d+m)_(\w+)"


    match = re.search(pattern, irods_file_path)

    if match:
        season = match.group(1)
        crop_type = match.group(2)
        year = match.group(3)
        level = match.group(4)
        instrument = match.group(5)
        scan_date = match.group(6)
        location = match.group(7)
        drone_type = match.group(8)
        altitude_m = int(match.group(9).replace("m", ""))
        camera_type = match.group(10)
    else:
        print("Invalid file path.")
        return

    # Remove the first column
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    # Change all NaN to NA for string columns only
    df = df.apply(lambda x: x.fillna("NA") if x.dtype == "object" else x)
    # fillna for rep
    df["rep"] = df["rep"].fillna(0).astype(int)
    # Convert the dataframe to a dictionary
    data = df.to_dict(orient="records")

    data = [
        {
            **data_point,
            "season": season,
            "crop_type": crop_type,
            "year": year,
            "level": level,
            "instrument": instrument,
            "scan_date": parse(scan_date).strftime('%Y%m%dT%H%M%S.%f%z') + "-0700",
            "sensor": "drone",
            "gantry_location": location,
            "drone_type": drone_type,
            "altitude_m": altitude_m,
            "camera_type": camera_type,
            "genotype": "_".join(data_point["accession"].strip().split(" ")) + "_" + str(data_point["plot"]),
          
        } for data_point in data
    ]

    return data

def get_all_tar_files(parent_dir: str):
    """
    Get all the tar files in the all the subdirectories of the parent directory.

    Parameters:
    - parent_dir (str): The parent directory to search for tar files.
    """
    tar_files = []
    with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
        parent_dir = session.collections.get(parent_dir)
        for sub_dir in parent_dir.subcollections:
            for tar_file in sub_dir.data_objects:
                if tar_file.name.endswith(".tar") and "tgi" in tar_file.name:
                    tar_files.append(f"{parent_dir.path}/{sub_dir.name}/{tar_file.name}")
    # print(tar_files)
    return tar_files

if __name__ == "__main__":
    # The output folder
    output_folder = "output/drone"
    # The path to the tar file in iRODS
    parent_dir = sys.argv[1]
    # Get all the tar files in the parent directory
    tar_files = get_all_tar_files(parent_dir)

    for irods_file_path in tar_files:
        # Extract the CSV file from the tar file
        df = extract_csv_from_tar_file(irods_file_path)
        # Get the output
        data = get_output(df, irods_file_path)

        # Save the output as JSON
        output_filename = f"{path.basename(irods_file_path)}.json"
        os.makedirs(output_folder, exist_ok=True)
        with open(path.join(output_folder, output_filename), "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

# /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/drone/sorghum/