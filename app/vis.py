"""
This module provides functionalities for visualizing PhytoOracle data using Streamlit and Plotly.
It connects to an OpenSearch instance to retrieve and filter data based on user inputs from the Streamlit sidebar.
The module includes functions to display data overviews, scan counts, and various visualizations.
Functions:
    - opensearch_connect(host, port, auth): Connects to an OpenSearch instance.
    - filters(): Retrieves filter options from the Streamlit sidebar.
    - get_data(client, crop_type, from_date, to_date, sensor_type, year, index_name): Retrieves data from OpenSearch based on filters.
    - get_scan_count(client, index_name, query): Aggregates and displays the number of records by instrument.
    - get_vis(client, index_name, query): Displays a line chart of data records by scan date for each instrument.
    - get_comparision_vis(client, index_name, query): Compares data records across selected sensors and seasons.
    - app(): Main function to run the Streamlit app.
"""


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
    crop_type = st.sidebar.multiselect('Crop Type', ['sorghum', 'lettuce', 'maize'])
    # Allow a date range for scanning (by date)
    scan_date_from = st.sidebar.date_input('Scan Date From', value=datetime.date(2018, 1, 1))
    scan_date_to = st.sidebar.date_input('Scan Date To', value=datetime.date(2030, 12, 31))
    sensor_type = st.sidebar.multiselect('Sensor Type', ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop'])
    year = st.sidebar.multiselect('Year', ['2022', '2021', '2020', '2019', '2018'])

    return crop_type, scan_date_from, scan_date_to, sensor_type, year

def get_data(client, crop_type, from_date, to_date, sensor_type, year, index_name):
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
        query['query']["bool"]["must"].append({"terms": {"crop_type": crop_type}})

    date_range_query = {
    }
    if from_date:
        date_range_query["gte"] = datetime.datetime.strftime(from_date, "%Y%m%dT%H%M%S.%f%z") + "-0700"
    if to_date:
        date_range_query["lte"] = datetime.datetime.strftime(to_date, "%Y%m%dT%H%M%S.%f%z") + "-0700"
    if from_date or to_date: 
        query['query']["bool"]["must"].append({"range": {"scan_date": date_range_query}})
    if sensor_type:
        query['query']["bool"]["must"].append({"terms": {"instrument": sensor_type}})
    if year:
        query['query']["bool"]["must"].append({"terms": {"year": year}})

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
            },
            "aggs": {
                "unique_files": {
                    "terms": {
                        "field": "file_path",
                        "size": 10000  # Adjust size as needed
                    },
                    "aggs": {
                        "total_file_size": {
                            "sum": {
                                "field": "file_size"
                            }
                        }
                    }
                },
                "unique_fieldbook_files": {
                    "terms": {
                        "field": "fieldbook_file_path",
                        "size": 10000  # Adjust size as needed
                    },
                    "aggs": {
                        "total_fieldbook_file_size": {
                            "sum": {
                                "field": "fieldbook_file_size"
                            }
                        }
                    }
                },
                "unique_entropy_files": {
                    "terms": {
                        "field": "entropy_file_name.keyword",
                        "size": 10000  # Adjust size as needed
                    },
                    "aggs": {
                        "total_entropy_file_size": {
                            "sum": {
                                "field": "entropy_file_size"
                            }
                        }
                    }
                }
            }
        }
    }

    query['size'] = 0

    response = client.search(index=index_name, body=query)
    
    # Put the data in a dataframe with rows instrument, number of scans, and total file size
    data = []

    total_scans = 0
    total_file_size = 0
    for instrument in response['aggregations']['by_instrument']['buckets']:
        # print(instrument)
        instrument_scans = instrument['doc_count']
        
        instrument_file_size = sum([file['total_file_size']['value'] for file in instrument['unique_files']['buckets']])
        instrument_fieldbook_file_size = sum([file['total_fieldbook_file_size']['value'] for file in instrument['unique_fieldbook_files']['buckets']])
        instrument_entropy_file_size = sum([file['total_entropy_file_size']['value'] for file in instrument['unique_entropy_files']['buckets']])
        
        total_instrument_file_size = instrument_file_size + instrument_fieldbook_file_size + instrument_entropy_file_size
        # print(instrument_file_size, instrument_fieldbook_file_size, instrument_entropy_file_size)
        data.append({
            'Instrument': instrument['key'],
            'Number of Scans': instrument_scans,
            'Total File Size (in bytes)': total_instrument_file_size
        })
        total_scans += instrument_scans
        total_file_size += total_instrument_file_size

    data.append({
        'Instrument': 'Total',
        'Number of Scans': total_scans,
        'Total File Size (in bytes)': total_file_size
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
    # resp_size = len(response['aggregations']['by_scan_date']['buckets'])

    
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


def get_comparision_vis(client, index_name, query):
    # Ask the user what sensor(s) they want to compare across which season(s)
    sensors = st.multiselect('Select the sensors to visualize', ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop'])
    # Find the year array from the query
    years = [2022, 2021, 2020, 2019, 2018]

    for filter in query['query']['bool']['must']:
        # st.write(filter)
        if 'terms' in filter and 'year' in filter['terms']:
            years = filter['terms']['year']

    seasons = st.multiselect('Select the seasons to compare', years)

    graph_type = st.selectbox('Select the graph type', ['Line', 'Bar', 'Scatter'])

    # Basically same thing as get_vis but with plot of a particular instrument showing up side by side for diff seasons (so x axis is date/month but not year)
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

    # Extract the year in a separate column
    df['year'] = pd.to_datetime(df['scan_date']).dt.year
    
    # Neutralize the year - make it 2000
    df['scan_date'] = pd.to_datetime(df['scan_date']).dt.strftime('%m-%d')
    df['scan_date'] = pd.to_datetime(df['scan_date'], format='%m-%d')

    # Array of DFs
    years = {}

    for season in seasons:
        if not years.get(season):
            years[season] = {}

        for sensor in sensors:
            years[season][sensor] = df[(df['year'] == int(season)) & (df['instrument'] == sensor)]

    try:
        # Plot the data
        figs = []
        if not sensors or len(sensors) == 0:
            raise Exception("No sensors selected")
        for sensor in sensors:
            # for each sensor, plot the data for each season (per single plot)
            fig = px.line(title=f'Scans available by date for {sensor}')
            df_combined = pd.DataFrame()
            for season in seasons:
                if season in years.keys() and sensor in years[season].keys():
                    df_combined = pd.concat([df_combined, years[season][sensor]])
            # Convert year to string
            df_combined['year'] = df_combined['year'].astype(str)
            match graph_type:
                case 'Bar':
                    fig = px.bar(df_combined, x='scan_date', y='count', color='year', title=f'Scans available by date for {sensor}')
                case 'Scatter':
                    fig = px.scatter(df_combined, x='scan_date', y='count', color='year', title=f'Scans available by date for {sensor}')
                case _:
                    fig = px.line(df_combined, x='scan_date', y='count', color='year', title=f'Scans available by date for {sensor}')


            figs.append(fig)

        for fig in figs:
            st.plotly_chart(fig)
    except Exception as e:
        missing_data = []   
        if len(sensors) == 0 or not sensors:
            missing_data.append("sensor")
        if len(seasons) == 0 or not seasons:
            missing_data.append("season")
        if len(graph_type) == 0 or not graph_type:
            missing_data.append("graph type")
        # st.write(missing_data)
        if len(missing_data) > 0:
            if len(missing_data) > 1:
                st.warning(f"Please enter at least one {', '.join(missing_data[:-1])} and {missing_data[-1]} to visualize the data.")
            else:
                st.warning(f"Please enter at least one {missing_data[0]} to visualize the data.")
        else:
            st.warning("Either no data is available or there was an error in processing the data.")





def app():
    global first_time
    if 'first_time' not in st.session_state:
        st.session_state.first_time = True
    first_time = st.session_state.first_time

    index_name = "phytooracle-index"
    host = os.getenv("ELASTIC_HOST")
    port = os.getenv("ELASTIC_PORT")
    auth = (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))

    # Connect to OpenSearch
    client = opensearch_connect(host, port, auth)

    st.title("PhytoOracle Analytics")
    
    crop_type, from_date, to_date, sensor_type, year = filters()
    # update_button = st.sidebar.button('Update')

    # Get the data
    # if update_button or first_time:
    if first_time:
        try:
            # first_time = False
            query = get_data(client, crop_type, from_date, to_date, sensor_type, year, index_name)
            get_scan_count(client, index_name, query)
            get_vis(client, index_name, query)
            get_comparision_vis(client, index_name, query)
        except Exception as e:
            st.warning("Either the data is not available or there was an error in processing the data.")


        

if __name__ == "__main__":
    app()