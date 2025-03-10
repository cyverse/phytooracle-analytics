"""
A sample file to upload data to index
"""
import os
import sys
from datetime import datetime
# Add the parent directory to the path to import the environment variables
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv

load_dotenv()

from opensearchpy import OpenSearch, helpers
import json

# paths: all files in output/ directory
paths = []
for root, dirs, files in os.walk("output/"):
    for file in files:
        if file.endswith(".json"):
            paths.append(os.path.join(root, file))
    for dir in dirs:
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(".json"):
                    paths.append(os.path.join(root, file))


print(f"Adding {len(paths)} files to the index.")

# Get connection details from environment variables
host = os.getenv("ELASTIC_HOST")
port = os.getenv("ELASTIC_PORT")
auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

print(host, port, auth)
# Check if the index exists, if not create it
client = OpenSearch(
    hosts=[{'host': host, 'port': port, 'scheme': 'http'}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    # use_ssl=True,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

index_name = "phytooracle-index"


if not client.indices.exists(index=index_name):
    print(f"The index '{index_name}' does not exist. Creating the index.")
    # Load the index mapping from a file
    with open("search_configuration/index_mapping.json", "r") as file:
        client.indices.create(index=index_name, body=json.load(file))
        
        
# Load AZMET data for 2020, 2021, 2022 in a single dictionary
azmet_data = {}
for year in range(2020, 2023):
    with open(f"azmet_output/{year}.json", 'r') as azmet_file:
        azmet_data[year] = {entry["day_of_year"]: entry for entry in json.load(azmet_file)}
# print(azmet_data)

print(paths)
for data_path in paths:
    print("Processing", data_path)
    with open(data_path, 'r') as file:
        data = json.load(file)
        print("Found data:", len(data))
        # Enrich data with AZMET weather data
        # "data" contains a field called scan_date
        # azmet data, found in azmet_output/ directory, is filtered by year, so all weather data from year 2020 is in azmet_output/2020.json
        # "data" is in the format: {scan_date: scan_date, ...}, eg. 20220512T000000.000000-0700
        # azmet data is in the format: {year: year, day_of_year: day_of_year....}
        # We need to convert the scan_date to a datetime object, extract the year and day_of_year, and then find the corresponding weather data in the azmet data
        # and then add the weather data to the "data" object

        for entry in data:
            try:
                scan_date = datetime.strptime(entry["scan_date"], "%Y%m%dT%H%M%S.%f%z")
                year = scan_date.year
                day_of_year = scan_date.timetuple().tm_yday
                
                # print(f"Extracting data for {year} and day of year {day_of_year}")
                # Find the corresponding weather data in the azmet data
                weather_data = azmet_data[year][str(day_of_year)]
                
                for key, value in weather_data.items():
                    entry[f"azmet_{key}"] = value

    
            except ValueError:
                print(f"Could not convert {entry['scan_date']} to a datetime object.")
                continue

        # # Convert all scan dates to datetime objects and then to isoformat
        # for entry in data:
        #     try:
        #         entry["scan_date"] = datetime.strptime(entry["scan_date"], "%Y%m%dT%H%M%S.%f%z").isoformat()
        #     except ValueError:
        #         print(f"Could not convert {entry['scan_date']} to a datetime object.")
        #         continue
    # Convert data for bulk indexing

    print(f"Linked data from file {data_path} to AZMET data.")
    actions = [
        {
            "_index": index_name,
            "_source": entry
        }
        for entry in data
    ]
    with open('actions.json', 'w') as file:
        json.dump(actions, file, indent=4)

    try:
        response = client.index(index=index_name, body=data[0])
        success, failed = helpers.bulk(client, actions)
        print(f"Successfully indexed {success} documents. with {data_path}")
        print(f"Failed to index {failed} documents.")
    except Exception as e:
        print(f"An error occurred while indexing the data: {e}")
        # Print the error in detail
        print(e)