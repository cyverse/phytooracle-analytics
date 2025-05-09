import os
import sys
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# Add the parent directory to the path to import the environment variables
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables from .env file
load_dotenv()

# Get connection details from environment variables
host = os.getenv("ELASTIC_HOST")
port = os.getenv("ELASTIC_PORT")
auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

# Create the OpenSearch client with SSL/TLS and hostname verification disabled
client = OpenSearch(
    hosts=[{'host': host, 'port': port, 'scheme': 'https'}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

if len(sys.argv) != 2:
    print("Usage: python delete_index.py <index_name>")
    sys.exit(1)

index_name = sys.argv[1]

# Check if the index exists
if client.indices.exists(index=index_name):
    print(f"The index '{index_name}' exists.")

    # Delete the index
    response = client.indices.delete(index=index_name)

    # Print the response
    print(f"Deleted the index '{index_name}'. Response: {response}")
else:
    print(f"The index '{index_name}' does not exist.")