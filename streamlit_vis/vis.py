import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px

# Add the parent directory to the path to import the environment variables
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from opensearchpy import OpenSearch


# Load the environment variables
from dotenv import load_dotenv
import datetime
load_dotenv()



# Get connected to opensearch
def opensearch_connect(host, port, auth):
    # Create the client with SSH/TLS and hostname verification disabled
    client = OpenSearch(
        hosts=[{'host': host, 'port': port, 'scheme': 'https'}],
        http_compress=True,  # enables gzip compression for request bodies
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    return client


# Get the filters from the sidebar
def filters():
    # Add a title
    st.sidebar.title('Filters')
    # Filter for Crop Type -dropdown
    crop_type = st.sidebar.selectbox('Crop Type', [None, 'sorghum', 'lettuce', 'maize'])
    # Allow a date range for scanning (by date)
    scan_date_from = st.sidebar.date_input('Scan Date From', value=datetime.date(2018, 1, 1))
    scan_date_to = st.sidebar.date_input('Scan Date To', value=datetime.date(2030, 12, 31))
    sensor_type = st.sidebar.selectbox('Sensor Type', [None, 'flirIrCamera', 'scanner3DTop', 'drone'])


    return crop_type, scan_date_from, scan_date_to, sensor_type

def get_data(client, crop_type, from_date, to_date, sensor_type, index_name):
    print(crop_type, from_date, to_date, sensor_type)

    # Define the query - to include basic filters for rest of the data
    query = {
        "query": {
            "bool": {
                "must": []
            }
        }
    }

    if crop_type:
        query['query']["bool"]["must"].append({"match": {"crop_type": crop_type}})

    date_range_query = {
    }
    if from_date:
        date_range_query["gte"] = datetime.datetime.strftime(from_date, "%Y%m%dT%H%M%S.%f%z") + "-0700"
    if to_date:
        date_range_query["lte"] = datetime.datetime.strftime(to_date, "%Y%m%dT%H%M%S.%f%z") + "-0700"
    if from_date or to_date: 
        query['query']["bool"]["must"].append({"range": {"scan_date": date_range_query}})
    if sensor_type:
        query['query']["bool"]["must"].append({"match": {"instrument": sensor_type}})

    # Add a temporary size to get the data
    query['size'] = 5

    st.write("Data Overview")
    data = client.search(index=index_name, body=query)
    df = pd.DataFrame([hit['_source'] for hit in data['hits']['hits']])
    st.dataframe(df)

    # Remove the size
    del query['size']

    return query

def get_scan_count(client, index_name, query):
    # Aggregate the number of records by instrument
    query['aggs'] = {
        "by_instrument": {
            "terms": {
                "field": "instrument"
            }
        }
    }

    query['size'] = 0

    response = client.search(index=index_name, body=query)
    
    # Put the data in a dataframe with rows instrument, number of scans
    data = []

    total_scans = 0
    for instrument in response['aggregations']['by_instrument']['buckets']:
        data.append({
            'Instrument': instrument['key'],
            'Number of Scans': instrument['doc_count']
        })
        total_scans += instrument['doc_count']

    data.append({
        'Instrument': 'Total',
        'Number of Scans': total_scans
    })

    
    df = pd.DataFrame(data)
    st.write("Scan Count by Instrument")
    st.dataframe(df)


def get_vis(client, index_name, query):
    # Get an aggregate of count of data records by scan date for each instrument
    
    query['aggs'] = {
        "by_scan_date": {
            "date_histogram": {
                "field": "scan_date",
                "calendar_interval": "day",
                "format": "yyyy-MM-dd"
            },
            "aggs": {
                "by_instrument": {
                    "terms": {
                        "field": "instrument"
                    }
                }
            }
        }
    }

    query['size'] = 0
    

    response = client.search(index=index_name, body=query)
    # Check the response size
    resp_size = len(response['aggregations']['by_scan_date']['buckets'])

    
    # Retrieve the data to a dataframe with columns: scan_date, instrument, count
    data = []
    for date in response['aggregations']['by_scan_date']['buckets']:
        for instrument in date['by_instrument']['buckets']:
            data.append({
                'scan_date': date['key_as_string'],
                'instrument': instrument['key'],
                'count': instrument['doc_count']
            })
    df = pd.DataFrame(data)
    
    # Plot the data
    fig = px.line(df, x='scan_date', y='count', color='instrument', title='Scans available by date')

    st.plotly_chart(fig)

    


def app():
    index_name = "phytooracle-index"
    host = os.getenv("ELASTIC_HOST")
    port = os.getenv("ELASTIC_PORT")
    auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

    # Connect to OpenSearch
    client = opensearch_connect(host, port, auth)

    st.title("PhytoOracle Analytics")
    
    crop_type, from_date, to_date, sensor_type = filters()
    update_button = st.sidebar.button('Update')

    # Get the data
    if update_button:
        query = get_data(client, crop_type, from_date, to_date, sensor_type, index_name)
        get_scan_count(client, index_name, query)
        get_vis(client, index_name, query)

if __name__ == "__main__":
    app()