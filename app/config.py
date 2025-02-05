# config.py
import os
from dotenv import load_dotenv

# Load the environment variables
load_dotenv()

# OpenSearch/Elastic configuration
INDEX_NAME = "phytooracle-index"
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_PORT = int(os.getenv("ELASTIC_PORT", "9200"))
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
