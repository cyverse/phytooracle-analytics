import os
import sys

# Add the parent directory to the path to import the environment variables
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
from opensearchpy import OpenSearch

load_dotenv()

host = os.getenv("ELASTIC_HOST") 
port = os.getenv("ELASTIC_PORT")
auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port, 'scheme': 'https'}],
    http_compress=True, # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

index_name = "phytooracle-index"

# Check if the index exists
if client.indices.exists(index=index_name):
    print(f"The index '{index_name}' exists.")
    # List the first 2 documents in the index, as well as the total number of documents
    response = client.search(index=index_name, size=2)
    print(f"First 2 documents in the index '{index_name}':")
    for hit in response['hits']['hits']:
        print(hit)
    print(f"Total number of documents in the index '{index_name}': {response['hits']['total']['value']}")
else:
    print(f"The index '{index_name}' does not exist.")


