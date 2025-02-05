import os
import sys
import csv
from opensearchpy import OpenSearch, helpers
from dotenv import load_dotenv
import json

# Load environment variables
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv()

# Get connection details from environment variables
host = os.getenv("ELASTIC_HOST")
port = os.getenv("ELASTIC_PORT")
auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

# Initialize OpenSearch client
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

# Query all data from the index
def fetch_all_data(client, index_name):
    try:
        # Use a scroll query to fetch all documents
        all_docs = []
        query = {
            "query": {"match_all": {}}
        }
        response = client.search(index=index_name, body=query, scroll="2m", size=1000)
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        all_docs.extend(hits)
        print(f"\rFetched {len(all_docs)} documents so far...", end='', flush=True)

        while hits:
            scroll_response = client.scroll(scroll_id=scroll_id, scroll="2m")
            hits = scroll_response["hits"]["hits"]
            if not hits:
                break
            all_docs.extend(hits)
            print(f"\rFetched {len(all_docs)} documents so far...", end='', flush=True)

        print()  # Move to the next line after completion
        return all_docs
    except Exception as e:
        print(f"\nError fetching data: {e}")
        return []

# Write data to CSV
def write_to_csv(docs, output_file):
    try:
        # Extract all unique fields from the documents
        fieldnames = set()
        for doc in docs:
            fieldnames.update(doc["_source"].keys())
        
        fieldnames = sorted(fieldnames)  # Optional: sort the field names
        
        # Write the data to a CSV file
        with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            
            for doc in docs:
                writer.writerow(doc["_source"])
        
        print(f"Data successfully written to {output_file}")
    except Exception as e:
        print(f"Error writing data to CSV: {e}")

# Fetch data and write to CSV
if __name__ == "__main__":
    print("Fetching data from the index...")
    documents = fetch_all_data(client, index_name)
    if documents:
        print(f"Fetched {len(documents)} documents in total.")
        output_file = "index_data.csv"
        print(f"Writing data to {output_file}...")
        write_to_csv(documents, output_file)
    else:
        print("No documents found or an error occurred.")
