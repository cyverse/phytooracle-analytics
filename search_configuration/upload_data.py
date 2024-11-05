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

# Check if the index exists, if not create it
client = OpenSearch(
    hosts=[{'host': host, 'port': port, 'scheme': 'https'}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
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

print(paths)
for data_path in paths:
    with open(data_path, 'r') as file:
        data = json.load(file)
        
        # # Convert all scan dates to datetime objects and then to isoformat
        # for entry in data:
        #     try:
        #         entry["scan_date"] = datetime.strptime(entry["scan_date"], "%Y%m%dT%H%M%S.%f%z").isoformat()
        #     except ValueError:
        #         print(f"Could not convert {entry['scan_date']} to a datetime object.")
        #         continue
    # Convert data for bulk indexing
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