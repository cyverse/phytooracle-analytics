"""
This script combines information from a fieldbook file with an entropy tar file, producing a unified
JSON file for indexing a single scan date. To cover all scan dates, execute this script multiple
times and changing the entropy_file_path argument.
"""

from os import path
import sys
# import csv
import re
import json
import os
import tarfile
import tempfile
import pandas as pd
from irods.session import iRODSSession

try:
    _IRODS_ENV_FILE = os.environ['IRODS_ENVIRONMENT_FILE']
except KeyError:
    _IRODS_ENV_FILE = path.expanduser('~/.irods/irods_environment.json')


# def parse_fieldbook_csv_file(fieldbook_csv_path: str) -> dict:
#     """
#     Parses the fieldbook CSV file into a dictionary with plant names as keys.
#     Each value is a dictionary that contains details about the plant's fieldbook data.

#     Parameters:
#     - fieldbook_csv_path (str): The file path to the CSV file to be parsed.

#     Returns:
#         A dictionary with plant names as keys and a dictionary of their corresponding fieldbook
#         data as values. If a plant name is repeated, only the first occurrence is added to the
#         dictionary. e.g.

#         {
#             'year': 2019,
#             'experiment': "lettuce_season_1",
#             'field': "south",
#             'treatment': "treat1",
#             'rep': "",
#             'range': 2,
#             'column': 4,
#             'plot': 204,
#             'type': "border",
#             'genotype': "Green_Towers_BORDER"
#         }
#     """
#     fieldbook_dict = {}

#     with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
#         with session.data_objects.open(fieldbook_csv_path, 'r') as csv_file:
#             reader = csv.reader(csv_file)
#             next(reader, None)  # Skip header
#             for row in reader:
#                 try:
#                     field_entry = {
#                         "year": int(row[0]),
#                         "experiment": row[1],
#                         "field": row[2],
#                         "treatment": row[3],
#                         "rep": row[4],
#                         "range": int(row[4]),
#                         "column": int(row[6]),
#                         "plot": int(row[7]),
#                         "type": row[8],
#                         "genotype": row[9]
#                     }
#                     plant_name = row[10]
#                     if plant_name in fieldbook_dict:
#                         print(
#                             f"Unexpected - Duplicate plant name found: {plant_name}. Ignoring it.")
#                     else:
#                         fieldbook_dict[plant_name] = field_entry
#                 except ValueError as ve:
#                     print(f"Issue with the number of columns or data conversion. Exiting!! \n{ve}")
#                     sys.exit(1)
#         return fieldbook_dict





# NOTE: This is supposed to be a temporary implemenation. The final implementation should use the simpler csv module.
# For some reason, currently, the csv module is claiming that the file has been provided as a binary file, not string. 
# This is a workaround to get the data from the file - so we can focus on the main task.
def parse_fieldbook_csv_file(fieldbook_csv_path: str) -> dict:
    """
    Parses the fieldbook CSV file into a dictionary with plant names as keys.
    Each value is a dictionary that contains details about the plant's fieldbook data.

    Parameters:
    - fieldbook_csv_path (str): The file path to the CSV file to be parsed from iRODS.

    Returns:
        A dictionary with plant names as keys and a dictionary of their corresponding fieldbook
        data as values.
    """
    fieldbook_dict = {}

    try:
        # Access the file using iRODS
        with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
            with session.data_objects.open(fieldbook_csv_path, 'r') as csv_file:
                # Use pandas to read the CSV content
                df = pd.read_csv(csv_file, sep=",")  # Adjust the separator if needed
                # Drop duplicate plant names, keeping the first occurrence
                df = df.drop_duplicates(subset=['plant_name'], keep='first')

                # Convert specific columns to the appropriate data types
                df['year'] = df['year'].astype(int)
                df['range'] = df['range'].astype(int)
                df['column'] = df['column'].astype(int)
                df['plot'] = df['plot'].astype(int)

                # Convert the DataFrame into a dictionary of dictionaries
                fieldbook_dict = df.set_index('plant_name').T.to_dict()

                # Change all nan values to None
                for plant_name, plant_dict in fieldbook_dict.items():
                    fieldbook_dict[plant_name] = {
                        k: v if pd.notna(v) else None for k, v in plant_dict.items()
                    }

        return fieldbook_dict

    except FileNotFoundError as fe:
        print(f"File not found: {fe}")
        return {}

    except pd.errors.EmptyDataError:
        print("The file is empty or invalid.")
        return {}

    except ValueError as ve:
        print(f"Data conversion issue: {ve}")
        return {}

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}

def download_and_extract_entropy_tar_file(irods_file_path: str) -> list[str]:
    """
    Downloads a tar file from an iRODS collection, extracts it, and returns a list containing the
    file names inside the tar file.

    Parameters:
    - irods_file_path (str): The iRODS path to the tar file.
    - irods_env_file (str): Path to the iRODS environment file.

    Returns:
    - file_names (list[string]): List containing the file names inside the tar file.
    """
    with iRODSSession(irods_env_file=_IRODS_ENV_FILE) as session:
        with tempfile.TemporaryDirectory() as tmpdir_name:
            local_tar_path = os.path.join(tmpdir_name, "entropy_tar.tar")
            obj = session.data_objects.get(irods_file_path)
            replica = None
            for r in obj.replicas:
                if r.status.isnumeric() and int(r.status) == 1:
                    replica = r
                    break
            # Read data from the iRODS object into a buffer
            data_buffer = bytearray(replica.size)
            num_bytes_read = obj.open('r').readinto(data_buffer)
            if num_bytes_read != replica.size:
                raise RuntimeError("Failed to read the entropy tar file, Exiting!!")
            # Write the buffer to the temporary file
            with open(local_tar_path, 'wb') as temp_file:
                temp_file.write(data_buffer)
            # Open the downloaded tar file, extract its contents, and list the names of the files
            # inside
            with tarfile.open(local_tar_path) as tar:
                tar.extractall(path=tmpdir_name)
                file_names = tar.getnames()  # List the file names after extracting
        return file_names


def parse_url_details(url: str) -> dict:
    """
    Parses the URL to extract season, crop_type, level, and instrument information.
    Try to match with two patterns, one contains the date in ISO format and other does not.
    Parameters:
    - url (str): The tar file's URL to parse.

    Returns:
    - dict: A dictionary containing the extracted details.
    """
    pattern = (
        r"/season_([0-9]+)_([a-zA-Z]+)_yr_[0-9]+" +
        r"/level_([0-9]+)" +
        r"/([^/]+)" +
        r"/[^w]+?" +
        r"/([0-9]{4})-([0-9]{2})-([0-9]{2})__([0-9]{2})-([0-9]{2})-([0-9]{2})-([0-9]{3})")
    match = re.search(pattern, url)
    if match:
        season, crop_type, level, instrument, yyyy, mon, dd, hh, mm, ss, sss = match.groups()
        return {
            "season": int(season),
            "crop_type": crop_type,
            "level": int(level),
            "instrument": instrument,
            "YYYY": yyyy,
            "MM": mon,
            "DD": dd,
            "hh": hh,
            "mm": mm,
            "ss": ss,
            "sss": sss
        }

    pattern = (
        r"/season_([0-9]+)_([a-zA-Z]+)_yr_[0-9]+" +
        r"/level_([0-9]+)" +
        r"/([^/]+)" +
        r"/[^w]+?" +
        r"/([0-9]{4})-([0-9]{2})-([0-9]{2})")
    match = re.search(pattern, url)
    if match:
        season, crop_type, level, instrument, yyyy, mon, dd = match.groups()
        return {
            "season": int(season),
            "crop_type": crop_type,
            "level": int(level),
            "instrument": instrument,
            "YYYY": yyyy,
            "MM": mon,
            "DD": dd,
            "hh": "00",
            "mm": "00",
            "ss": "00",
            "sss": "000"
        }

    # Raise an error if the URL doesn't match the expected pattern
    raise RuntimeError("Failed to parse URL. Exiting!!")


def _parse_entropy_tar_file(fieldbook_dict, csv_file_names, parsed_url):
    json_list = []
    null_rows = set()
    for csv_file_name in csv_file_names:
        if not csv_file_name.endswith(".csv"):
            continue
        plant_name = csv_file_name.removesuffix("_volumes_entropy.csv")
        match = re.search(r'\/(.+?)_+[0-9]+$', plant_name)
        genotype = match.group(1) if match else plant_name
        plant_name = plant_name.split("/")[1]
        if plant_name in fieldbook_dict:
            scan_date = (
                parsed_url['YYYY'] + parsed_url['MM'] + parsed_url['DD'] + 'T' +
                parsed_url['hh'] +
                parsed_url['mm'] +
                parsed_url['ss'] + '.' + parsed_url['sss'] +
                '-0700')
            fb_info = fieldbook_dict[plant_name]
            plant_dict = {
                "plant_name": plant_name,
                "genotype": genotype,
                "season": parsed_url["season"],
                "crop_type": parsed_url["crop_type"],
                "year_of_planting": fb_info["year"],
                "level": parsed_url["level"],
                "instrument": parsed_url["instrument"],
                "scan_date": scan_date,
                "field": fb_info["field"],
                "experiment": fb_info["experiment"],
                "treat": fb_info["treatment"],
                "rep": fb_info["rep"],
                "range": fb_info["range"],
                "column": fb_info["column"],
                "plot": fb_info["plot"],
                "id": f"{plant_name}_{scan_date}"
            }

            # Convert all NaN values to None
            for key, value in plant_dict.items():
                if pd.isna(value):
                    plant_dict[key] = None

            json_list.append(plant_dict)
            # Get all the rows with NaN values in plant_dict
            null_rows.update({k for k, v in plant_dict.items() if pd.isna(v)})
        else:
            print(f"{plant_name} not found in fieldbook. Check fieldbook data or plant name.")
            print(f"Ignoring {plant_name}")

    print(f"Null rows: {null_rows}")
    # Create the output directory if it doesn't exist
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Writing the combined information to a JSON file to be index ready by OpenSearch
    with open(
        path.join(output_dir, f"combined_plants_info_{scan_date}.json"), 'w', encoding='utf-8'
    ) as json_file:
        json.dump(json_list, json_file, indent=4)


def main(fieldbook_csv_path: str, entropy_file_path: str) -> None:
    """
    Parameters:
    - fieldbook_csv_path: Absolute path of the fieldbook CSV file in iRODS
    - entropy_file_path: Absolute path of the entropy.tar file corresponding to a scan date in iRODS

    Returns:
    - None
    Generates output/file.json file after successful completion of the script
    """
    # Parse the fieldbook
    fieldbook_dict = parse_fieldbook_csv_file(fieldbook_csv_path)
    # print(fieldbook_dict)
    # Parse the entropy file
    csv_file_names = download_and_extract_entropy_tar_file(entropy_file_path)
    # Parse the URL
    parsed_url = parse_url_details(entropy_file_path)
    # Combine everything above
    _parse_entropy_tar_file(fieldbook_dict, csv_file_names, parsed_url)


if __name__ == "__main__":
    # Check if the correct number of arguments are provided
    if len(sys.argv) != 3:
        print("Usage: python data_preparation.py <fieldbook_csv_path> <entropy_file_path>")
        sys.exit(1)

    if sys.argv[1] == "--help" or sys.argv[1] == "-h":
        print("Usage: python script.py <fieldbook_csv_path> <entropy_file_path>")
        print("  fieldbook_csv_path: Absolute path of the fieldbook CSV file in iRODS")
        print("  entropy_file_path: Absolute path of the entropy.tar file corresponding to a " +
              "scan date in iRODS")
        sys.exit(0)

    main(fieldbook_csv_path=sys.argv[1], entropy_file_path=sys.argv[2])
