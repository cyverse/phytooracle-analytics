# visualizations.py
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

def get_scan_count(client, index_name, query):
    """
    Aggregate and display the number of records by instrument.
    """
    query['aggs'] = {
        "by_instrument": {
            "terms": {"field": "instrument"},
            "aggs": {
                "unique_files": {
                    "terms": {"field": "file_path", "size": 10000},
                    "aggs": {"total_file_size": {"sum": {"field": "file_size"}}}
                },
                "unique_fieldbook_files": {
                    "terms": {"field": "fieldbook_file_path", "size": 10000},
                    "aggs": {"total_fieldbook_file_size": {"sum": {"field": "fieldbook_file_size"}}}
                },
                "unique_entropy_files": {
                    "terms": {"field": "entropy_file_name.keyword", "size": 10000},
                    "aggs": {"total_entropy_file_size": {"sum": {"field": "entropy_file_size"}}}
                }
            }
        }
    }
    query["size"] = 0

    response = client.search(index=index_name, body=query)
    data = []
    total_scans = 0
    total_file_size = 0
    for instrument in response['aggregations']['by_instrument']['buckets']:
        instrument_scans = instrument['doc_count']
        instrument_file_size = sum(bucket['total_file_size']['value'] for bucket in instrument['unique_files']['buckets'])
        instrument_fieldbook_file_size = sum(bucket['total_fieldbook_file_size']['value'] for bucket in instrument['unique_fieldbook_files']['buckets'])
        instrument_entropy_file_size = sum(bucket['total_entropy_file_size']['value'] for bucket in instrument['unique_entropy_files']['buckets'])
        total_instrument_file_size = instrument_file_size + instrument_fieldbook_file_size + instrument_entropy_file_size

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
    st.subheader("Scan Count by Instrument")
    st.dataframe(df)

def get_vis(client, index_name, query):
    """
    Create a line chart of record counts by scan date for each instrument.
    """
    query['aggs'] = {
        "by_scan_date": {
            "date_histogram": {
                "field": "scan_date",
                "calendar_interval": "day",
                "format": "yyyy-MM-dd"
            },
            "aggs": {
                "by_instrument": {"terms": {"field": "instrument"}}
            }
        }
    }
    query["size"] = 0
    response = client.search(index=index_name, body=query)
    
    data = []
    for date_bucket in response['aggregations']['by_scan_date']['buckets']:
        for instrument in date_bucket['by_instrument']['buckets']:
            data.append({
                'scan_date': date_bucket['key_as_string'],
                'instrument': instrument['key'],
                'count': instrument['doc_count']
            })
    st.subheader("Record Counts by Scan Date")
    df = pd.DataFrame(data)
    fig = px.line(df, x='scan_date', y='count', color='instrument')
    st.plotly_chart(fig)

def get_comparison_vis(client, index_name, query):
    """
    Compare scan data across selected sensors and seasons.
    """
    st.subheader("Compare scan data across selected sensors and seasons")
    sensors = st.multiselect('Select the sensors to visualize', ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop'])
    years = [2022, 2021, 2020, 2019, 2018]
    # Override years if provided in the query
    for f in query['query']['bool']['must']:
        if 'terms' in f and 'year' in f['terms']:
            years = f['terms']['year']

    seasons = st.multiselect('Select the seasons to compare', years)
    graph_type = st.selectbox('Select the graph type', ['Line', 'Bar', 'Scatter'])

    query['aggs'] = {
        "by_scan_date": {
            "date_histogram": {
                "field": "scan_date",
                "calendar_interval": "day",
                "format": "yyyy-MM-dd"
            },
            "aggs": {
                "by_instrument": {"terms": {"field": "instrument"}}
            }
        }
    }
    query["size"] = 0
    response = client.search(index=index_name, body=query)

    data = []
    for date_bucket in response['aggregations']['by_scan_date']['buckets']:
        for instrument in date_bucket['by_instrument']['buckets']:
            data.append({
                'scan_date': date_bucket['key_as_string'],
                'instrument': instrument['key'],
                'count': instrument['doc_count']
            })
    df = pd.DataFrame(data)
    df['year'] = pd.to_datetime(df['scan_date']).dt.year
    df['scan_date'] = pd.to_datetime(df['scan_date']).dt.strftime('%m-%d')
    df['scan_date'] = pd.to_datetime(df['scan_date'], format='%m-%d')

    # Organize data by season and sensor
    seasons_dict = {}
    for season in seasons:
        seasons_dict[season] = {}
        for sensor in sensors:
            seasons_dict[season][sensor] = df[(df['year'] == int(season)) & (df['instrument'] == sensor)]
    
    figs = []
    try:
        if not sensors:
            raise Exception("No sensors selected")
        for sensor in sensors:
            df_combined = pd.DataFrame()
            for season in seasons:
                if season in seasons_dict and sensor in seasons_dict[season]:
                    df_combined = pd.concat([df_combined, seasons_dict[season][sensor]])
            df_combined['year'] = df_combined['year'].astype(str)
            if graph_type == 'Bar':
                fig = px.bar(df_combined, x='scan_date', y='count', color='year',
                             title=f'Scans available by date for {sensor}')
            elif graph_type == 'Scatter':
                fig = px.scatter(df_combined, x='scan_date', y='count', color='year',
                                 title=f'Scans available by date for {sensor}')
            else:
                fig = px.line(df_combined, x='scan_date', y='count', color='year',
                              title=f'Scans available by date for {sensor}')
            figs.append(fig)
        for fig in figs:
            st.plotly_chart(fig)
    except Exception as e:
        missing = []
        if not sensors:
            missing.append("sensor")
        if not seasons:
            missing.append("season")
        if not graph_type:
            missing.append("graph type")
        if missing:
            if len(missing) > 1:
                st.warning(f"Please enter at least one {', '.join(missing[:-1])} and {missing[-1]} to visualize the data.")
            else:
                st.warning(f"Please enter at least one {missing[0]} to visualize the data.")
        else:
            st.warning("Either no data is available or there was an error processing the data.")

def get_vis_over_time(client, index_name, query):
    """
    Visualize selected column(s) over time using a user‐chosen graph type.
    """
    cols_to_vis = st.selectbox('Select the columns to visualize',
                               ['accession', 'crop_type', 'altitude_m', 'bounding_area_m2',
                                'file_size', 'mean_tgi', 'q1_tgi', 'q3_tgi', 'roi_temp'])
    sensors = st.multiselect('Select the sensors to visualize',
                             ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop'],
                             key='sensors_to_visualize')
    graph_type = st.selectbox('Select the graph type', ['Line', 'Bar', 'Scatter'], key='graph_type')
    
    # Ensure the query uses the selected sensors
    instrument_term_found = False
    for f in query['query']['bool']['must']:
        if 'terms' in f and 'instrument' in f['terms'] and sensors:
            f['terms']['instrument'] = sensors
            instrument_term_found = True
    if not instrument_term_found and sensors:
        query['query']['bool']['must'].append({"terms": {"instrument": sensors}})
    
    cols_to_vis = [cols_to_vis]
    unq_count_values = [col for col in cols_to_vis if col in ['accession', 'crop_type']]
    
    query['aggs'] = {
        "by_scan_date": {
            "date_histogram": {
                "field": "scan_date",
                "calendar_interval": "day",
                "format": "yyyy-MM-dd"
            },
            "aggs": {}
        }
    }
    for col in unq_count_values:
        query['aggs']['by_scan_date']['aggs'][col] = {"terms": {"field": f"{col}.keyword"}}
    
    query["size"] = 0
    numeric_cols = [col for col in cols_to_vis if col not in unq_count_values]
    for col in numeric_cols:
        query['aggs']['by_scan_date']['aggs'][col] = {"avg": {"field": col}}
    
    response = client.search(index=index_name, body=query)
    
    data = []
    for bucket in response['aggregations']['by_scan_date']['buckets']:
        row = {'scan_date': bucket['key_as_string']}
        for col in unq_count_values:
            row[col] = len(bucket[col]['buckets'])
        for col in numeric_cols:
            row[col] = bucket[col]['value']
        data.append(row)
    
    df = pd.DataFrame(data)
    df['year'] = pd.to_datetime(df['scan_date']).dt.year
    st.write(df)
    try:
        if not sensors:
            raise Exception("No sensors selected")
        if not graph_type:
            raise Exception("No graph type selected")
        if graph_type == 'Bar':
            fig = px.bar(df, x='scan_date', y=cols_to_vis, title='Scans available by date')
        elif graph_type == 'Scatter':
            fig = px.scatter(df, x='scan_date', y=cols_to_vis, title='Scans available by date')
        else:
            fig = px.line(df, x='scan_date', y=cols_to_vis, title='Scans available by date')
        st.plotly_chart(fig)
    except Exception as e:
        missing = []
        if not sensors:
            missing.append("sensor")
        if not graph_type:
            missing.append("graph type")
        if missing:
            if len(missing) > 1:
                st.warning(f"Please enter at least one {', '.join(missing[:-1])} and {missing[-1]} to visualize the data.")
            else:
                st.warning(f"Please enter at least one {missing[0]} to visualize the data.")
        else:
            st.warning("Either no data is available or there was an error processing the data.")

def visualize_parameters(client, index_name, query, get_all_columns_func):
    """
    Visualize the value of a specific parameter over time including statistics.
    """
    st.subheader("Visualize aggregated parameters over time")
    
    # Build a list of visualizable columns and extend with any azmet_ columns found
    visualizable_columns = ['roi_temp', 'bounding_area_m2', 'mean_tgi', 'q1_tgi', 'q3_tgi', 'box axis']
    all_columns = get_all_columns_func(client, index_name)
    azmet_columns = [col for col in all_columns if col.startswith('azmet_')]
    visualizable_columns.extend(azmet_columns)
    
    cols_to_vis = st.selectbox('Select the columns to visualize', visualizable_columns)
    if cols_to_vis == 'box axis':
        graph_options = ['Map']
    else:
        graph_options = ['Line', 'Box&Whisker', 'Scatter']
    graph_type = st.selectbox('Select the graph type', graph_options)
    year = st.selectbox('Select the year', ['2022', '2021', '2020', '2019', '2018'])
    crop = st.multiselect('Select the crop type  (optional)', ['sorghum', 'lettuce', 'maize'])
    
    # Add filters to the query
    query['query']['bool']['must'].append({"term": {"year": year}})
    if crop:
        query['query']['bool']['must'].append({"terms": {"crop_type": crop}})
    
    if cols_to_vis == 'box axis':
        # Ensure that coordinate fields exist
        query['query']['bool']['must'].append({"exists": {"field": "min_x"}})
        query['query']['bool']['must'].append({"exists": {"field": "min_y"}})
        query['query']['bool']['must'].append({"exists": {"field": "max_x"}})
        query['aggs'] = {
            "by_scan_date": {
                "date_histogram": {
                    "field": "scan_date",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "box_axis": {
                        "top_hits": {
                            "_source": {"includes": ["min_x", "min_y", "max_x", "max_y"]},
                            "size": 100
                        }
                    }
                }
            }
        }
    elif cols_to_vis.startswith('azmet_'):
        query['aggs'] = {
            "by_scan_date": {
                "date_histogram": {
                    "field": "scan_date",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "value": {
                        "top_hits": {
                            "_source": {"includes": [cols_to_vis]},
                            "size": 1
                        }
                    }
                }
            }
        }
        query["size"] = 0
    else:
        query['aggs'] = {
            "by_scan_date": {
                "date_histogram": {
                    "field": "scan_date",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "mean": {"avg": {"field": cols_to_vis}},
                    "median": {"percentiles": {"field": cols_to_vis, "percents": [50]}},
                    "max": {"max": {"field": cols_to_vis}},
                    "min": {"min": {"field": cols_to_vis}}
                }
            }
        }
        query["size"] = 0

    response = client.search(index=index_name, body=query)
    data = []

    if cols_to_vis == 'box axis':
        for bucket in response['aggregations']['by_scan_date']['buckets']:
            row = {'scan_date': bucket['key_as_string'], 'boxes': []}
            for hit in bucket['box_axis']['hits']['hits']:
                row['boxes'].append(hit['_source'])
            data.append(row)
    elif cols_to_vis.startswith('azmet_'):
        for bucket in response['aggregations']['by_scan_date']['buckets']:
            row = {'scan_date': bucket['key_as_string']}
            hits = bucket['value']['hits']['hits']
            if hits:
                row[cols_to_vis] = hits[0]['_source'][cols_to_vis]
            data.append(row)
    else:
        for bucket in response['aggregations']['by_scan_date']['buckets']:
            row = {
                'scan_date': bucket['key_as_string'],
                'mean': bucket['mean']['value'],
                'median': bucket['median']['values']['50.0'],
                'max': bucket['max']['value'],
                'min': bucket['min']['value']
            }
            data.append(row)
    
    df = pd.DataFrame(data)
    # st.write(df)
    df = df.dropna()

    if cols_to_vis == 'box axis':
        y_values = ['boxes']
    elif cols_to_vis.startswith('azmet_'):
        y_values = [cols_to_vis]
    else:
        y_values = ['mean', 'median', 'max', 'min']

    if graph_type == 'Map':
        date_choice = st.selectbox('Select a date to visualize the boxes',
                                   df[df['boxes'].apply(lambda x: len(x) > 0)]['scan_date'].unique())
        df = df[df['scan_date'] == date_choice]
        df_exploded = df.explode('boxes')
        df_exploded['min_y'] = df_exploded['boxes'].apply(lambda x: x.get('min_y') if isinstance(x, dict) else None)
        df_exploded['min_x'] = df_exploded['boxes'].apply(lambda x: x.get('min_x') if isinstance(x, dict) else None)
        df_exploded['max_y'] = df_exploded['boxes'].apply(lambda x: x.get('max_y') if isinstance(x, dict) else None)
        df_exploded['max_x'] = df_exploded['boxes'].apply(lambda x: x.get('max_x') if isinstance(x, dict) else None)
        df_exploded = df_exploded.dropna(subset=['min_y', 'min_x', 'max_y', 'max_x'])
        
        fig = go.Figure()
        for _, row in df_exploded.iterrows():
            fig.add_shape(
                type="rect",
                x0=row['min_x'],
                y0=row['min_y'],
                x1=row['max_x'],
                y1=row['max_y'],
                line=dict(color="RoyalBlue"),
                fillcolor="LightSkyBlue",
                opacity=0.5,
            )
        fig.update_layout(
            title=f"Boxes on {date_choice}",
            xaxis_title="X Coordinate",
            yaxis_title="Y Coordinate",
            xaxis=dict(scaleanchor="y", scaleratio=1),
            yaxis=dict(constrain='domain'),
            showlegend=False,
            autosize=True,
            height=800,
        )
        fig.update_xaxes(range=[df_exploded['min_x'].min(), df_exploded['max_x'].max()])
        fig.update_yaxes(range=[df_exploded['min_y'].min(), df_exploded['max_y'].max()])
    elif graph_type == 'Line':
        fig = px.line(df, x='scan_date', y=y_values, title='Value of parameters over time')
    elif graph_type == 'Box&Whisker':
        fig = px.box(df, x='scan_date', y=y_values, title='Value of parameters over time')
    else:
        fig = px.scatter(df, x='scan_date', y=y_values, title='Value of parameters over time')
    
    st.plotly_chart(fig)
