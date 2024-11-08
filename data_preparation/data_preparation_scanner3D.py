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

               
                print(df.head())
                # Convert specific columns to the appropriate data types
                df['year'] = df['year'].astype(int)
                df['range'] = df['range'].astype(int)
                # df['column'] = df['column'].astype(int)
                df['row'] = df['row'].astype(int)
                df['plot'] = df['plot'].astype(int)
                
                # Set df['rep'] to 0 if it is NaN
                df['rep'] = df['rep'].fillna(0).astype(int)
                
                # In any other column, replace nan with NA
                df = df.fillna('NA')

                # Add a new row df['uid'] = df['species'] + df['plot']
                df['uid'] = df['accession'] + "_" + df['plot'].astype(str)
                # remove any leading or trailing whitespaces from df['uid']
                df['uid'] = df['uid'].str.strip()
                # in df["uid"], replace any whitespace with an underscore
                df['uid'] = df['uid'].str.replace(" ", "_")

                # make all the column names lowercase
                df.columns = df.columns.str.lower()

                print(df.head())
                fieldbook_dict = df.set_index('uid').to_dict(orient='index')
                # Change all nan values to None
                for uid, plant_dict in fieldbook_dict.items():
                    fieldbook_dict[uid] = {
                        k: v if pd.notna(v) else None for k, v in plant_dict.items()
                    }

                # Add file metadata information : fieldbook_file_path & fieldbook_file_size
                fieldbook_dict["fieldbook_file_path"] = fieldbook_csv_path
                fieldbook_dict["fieldbook_file_size"] = session.data_objects.get(fieldbook_csv_path).size

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
                file_sizes = [tar.getmember(file_name).size for file_name in file_names]

        return file_names, file_sizes


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
    for csv_file_name, csv_file_size in zip(*csv_file_names):
        if not csv_file_name.endswith(".csv"):
            continue
        plant_name = csv_file_name.removesuffix("_volumes_entropy.csv")
        match = re.search(r'\/(.+?)_+[0-9]+$', plant_name)
        genotype = match.group(1) if match else plant_name
        plant_name = plant_name.split("/")[1]
        plant_fb_name = "_".join(plant_name.split("_")[:-1])
        if plant_fb_name in fieldbook_dict:
            scan_date = (
                parsed_url['YYYY'] + parsed_url['MM'] + parsed_url['DD'] + 'T' +
                parsed_url['hh'] +
                parsed_url['mm'] +
                parsed_url['ss'] + '.' + parsed_url['sss'] +
                '-0700')
            fb_info = fieldbook_dict[plant_fb_name]
            plant_dict = {
                "plant_name": plant_name,
                "genotype": genotype,
                "season": parsed_url["season"],
                "crop_type": parsed_url["crop_type"],
                "year_of_planting": fb_info["year"],
                "level": parsed_url["level"],
                "instrument": parsed_url["instrument"],
                "scan_date": scan_date,
                "species": fb_info["species"],
                "accession": fb_info["accession"],
                "fb_entry_id": fb_info["entry_id"],
                "seed_src_id": fb_info["seed-sourceid"],
                "replicated_in_2020": fb_info["replicated_in_2020"],
                "fieldbook_file_path": fieldbook_dict["fieldbook_file_path"],
                "fieldbook_file_size": fieldbook_dict["fieldbook_file_size"],
                "entropy_file_name": csv_file_name,
                "entropy_file_size": csv_file_size,
                # "experiment": fb_info["experiment"],
                "treat": fb_info["treatment"],
                "rep": fb_info["rep"],
                "range": fb_info["range"],
                "row": fb_info["row"],
                "fb_type": fb_info["type"],
                "plot": fb_info["plot"],
                "id": f"{plant_name}_{scan_date}",
                "sensor": "scanner3DTop"
            }

            # Convert all NaN values to None
            for key, value in plant_dict.items():
                if pd.isna(value):
                    plant_dict[key] = None

            json_list.append(plant_dict)
            # Get all the rows with NaN values in plant_dict
            null_rows.update({k for k, v in plant_dict.items() if pd.isna(v)})
        else:
            print(f"{plant_name}, identified by {plant_fb_name} not found in fieldbook. Check fieldbook data or plant name.")
            print(f"Ignoring {plant_name}")

    print(f"Null rows: {null_rows}")
    # Create the output directory if it doesn't exist
    output_dir = "output/Scanner3DTop"
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
    # Print all keys
    print("Fieldbook keys:")
    print(fieldbook_dict.keys())

    # print(fieldbook_dict)
    # Parse the entropy file
    csv_file_names = download_and_extract_entropy_tar_file(entropy_file_path)
    # Pretty print the first 5 entries of the csv file names
    print("First 5 entries of the csv file names:")
    print(csv_file_names[:5])
    # Parse the URL
    parsed_url = parse_url_details(entropy_file_path)
    # Pretty print the parsed URL
    print("Parsed URL:")
    print(parsed_url)
    # # Combine everything above
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


# python3 data_preparation/data_preparation.py /iplant/home/shared/phytooracle/season_10_lettuce_yr_2020/lettuce_field_book.csv /iplant/home/shared/phytooracle/season_10_lettuce_yr_2020/level_3/scanner3DTop/2020-01-23/individual_plants_out/2020-01-23_3d_volumes_entropy_v009.tar
# python3 data_preparation/data_preparation.py /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/North_gantry_fieldbook_2022_replants.csv /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/scanner3DTop/sorghum/2022-05-05__19-55-41-328_sorghum/individual_plants_out/2022-05-05__19-55-41-328_sorghum_3d_volumes_entropy_v009.tar