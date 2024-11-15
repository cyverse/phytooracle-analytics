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
            print(f"Accessed directory {directory}")
            # Loop through each data object (file) in the collection
            for obj in collection.subcollections:
                file_path = os.path.join(directory, obj.name)
                file_name = file_path.split("/")[-1] + "_3d_volumes_entropy_v009.tar"
                file_path+= "/individual_plants_out/"
                file_path+= file_name
                try: 
                    # Run the script on the file
                    print(f"Running script on {file_path}")
                    os.system(f"python3 data_preparation/helper/scanner3D.py {fieldbook_csv_path} {file_path}")
                except Exception as e:
                    print(f"An error occurred while running the script on {file_path}: {e}")

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



# python3 data_preparation/scanner3D.py /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/North_gantry_fieldbook_2022_replants.csv /iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/scanner3DTop/sorghum/
# python3 data_preparation/scanner3D.py /iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/Gantry_fieldbook_Aug-2020_Revised_Irr_TRT.csv /iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_2/scanner3DTop/