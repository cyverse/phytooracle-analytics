"""
A sample file to upload data to index
"""
from opensearchpy import OpenSearch, helpers
import json
paths = ["output/combined_plants_info_2023-01-23__18-26-23-632.json"]  # Dummy
for data_path in paths:
    with open(data_path, 'r') as file:
        data = json.load(file)
    
    # Convert data for bulk indexing
    actions = [
        {
            "_index": "phytooracle-index",
            "_source": entry
        }
        for entry in data
    ]
    with open('actions.json', 'w') as file:
        json.dump(actions, file, indent=4)

    host = 'localhost'
    port = 9200
    auth = ('admin', 'admin') # For testing only.

    # Create the client with SSL/TLS and hostname verification disabled.
    client = OpenSearch(
        hosts = [{'host': host, 'port': port, 'scheme': 'https'}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    response = client.index(index="phytooracle-index", body=data[0])
    success, failed = helpers.bulk(client, actions)
    print(f"Successfully indexed {success} documents. with {data_path}")
    print(f"Failed to index {failed} documents.")
