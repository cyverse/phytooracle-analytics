import os
import sys
from irods.session import iRODSSession
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist

def run_script_on_files(fieldbook_csv_path, directory):
    # Get iRODS environment file
    try:
        irods_env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    except KeyError:
        irods_env_file = os.path.expanduser('~/.irods/irods_environment.json')

    try:
        # Start iRODSSession to handle iRODS interaction
        with iRODSSession(irods_env_file=irods_env_file) as session:
            # Access the specified directory in iRODS
            collection = session.collections.get(directory)

            # Loop through each data object (file) in the collection
            for obj in collection.data_objects:
                file_path = os.path.join(directory, obj.name)
                
                try:
                    # Open the file in streaming mode to avoid loading it all into memory
                    with session.data_objects.open(file_path, 'r') as data_object:
                        # Construct the command to run the Python script, passing the file path as param
                        command = f"python3 data_preparation/data_preparation.py {fieldbook_csv_path} {file_path}"
                        print(f"Running command: {command}")
                        
                        # Run the command (without loading the file into memory)
                        os.system(command)
                
                except DataObjectDoesNotExist:
                    print(f"File {file_path} not found in iRODS.")
                    continue

    except CollectionDoesNotExist:
        print(f"The directory {directory} does not exist in iRODS.")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <fieldbook_csv_path> <irods_directory_path>")
        sys.exit(1)

    fieldbook_csv_path = sys.argv[1]
    irods_directory_path = sys.argv[2]

    run_script_on_files(fieldbook_csv_path, irods_directory_path)


# python3 data_preparation/bulk_add.py /iplant/home/shared/phytooracle/tmp/season10_rgb_3d_updated_na.csv /iplant/home/shared/phytooracle/season_10_lettuce_yr_2020/level_3/scanner3DTop/2020-01-23/individual_plants_out/