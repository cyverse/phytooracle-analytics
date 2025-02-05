# data.py
import datetime
import pandas as pd
import streamlit as st

def get_all_columns(client, index_name):
    """
    Retrieve and return a union of all fields across sensor types.
    """
    all_columns = set()
    sensor_types = ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop']

    for sensor in sensor_types:
        query = {
            "query": {
                "bool": {
                    "must": [{"term": {"instrument": sensor}}]
                }
            },
            "size": 1
        }
        data = client.search(index=index_name, body=query)
        for hit in data['hits']['hits']:
            all_columns.update(hit['_source'].keys())
    return all_columns

def get_data(client, crop_type, from_date, to_date, sensor_type, year, index_name):
    """
    Build a query from the filter options, show an overview of the returned data,
    and return the query (with filters) for further use.
    """
    query = {"query": {"bool": {"must": []}}}

    if crop_type:
        query["query"]["bool"]["must"].append({"terms": {"crop_type": crop_type}})
    
    date_range_query = {}
    if from_date:
        date_range_query["gte"] = from_date.strftime("%Y%m%dT%H%M%S.%f%z") + "-0700"
    if to_date:
        date_range_query["lte"] = to_date.strftime("%Y%m%dT%H%M%S.%f%z") + "-0700"
    if from_date or to_date:
        query["query"]["bool"]["must"].append({"range": {"scan_date": date_range_query}})
    
    if sensor_type:
        query["query"]["bool"]["must"].append({"terms": {"instrument": sensor_type}})
    
    if year:
        query["query"]["bool"]["must"].append({"terms": {"year": year}})

    # Temporarily limit the number of results for an overview
    query["size"] = 5

    st.write("Data Overview")
    data = client.search(index=index_name, body=query)
    df = pd.DataFrame([hit['_source'] for hit in data['hits']['hits']])
    st.dataframe(df)

    # Remove the temporary size limit
    del query["size"]

    return query
