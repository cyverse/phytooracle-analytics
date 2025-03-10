# visualizations.py
import math
import copy
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    Visualize the value of one or more parameters over time including statistics.
    
    - The "box axis" option is special and, if selected, must be the only option.
    - For all other columns (whether one or many are selected), the original query 
      structure is preserved. Each column is queried separately and then displayed 
      on a subplot with a shared time (x) axis.
    """
    st.subheader("Visualize aggregated parameters over time")
    
    # Build a list of visualizable columns and extend with any azmet_ columns found.
    visualizable_columns = ['roi_temp', 'bounding_area_m2', 'mean_tgi', 'q1_tgi', 'q3_tgi', 'box axis']
    all_columns = get_all_columns_func(client, index_name)
    azmet_columns = [col for col in all_columns if col.startswith('azmet_')]
    visualizable_columns.extend(azmet_columns)
    
    # Use a multiselect so the user can pick one or more columns.
    selected_columns = st.multiselect(
        'Select the columns to visualize', 
        visualizable_columns,
        default=[visualizable_columns[0]]
    )
    
    # Enforce the rule that if "box axis" is selected it must be the only option.
    if "box axis" in selected_columns and len(selected_columns) > 1:
        st.warning("You cannot combine 'box axis' with other columns. Ignoring 'box axis'.")
        selected_columns = [col for col in selected_columns if col != "box axis"]
    
    # Determine the available graph types.
    if len(selected_columns) == 1 and selected_columns[0] == "box axis":
        graph_options = ['Map']
    else:
        graph_options = ['Line', 'Box&Whisker', 'Scatter']
    graph_type = st.selectbox('Select the graph type', graph_options)
    
    # Additional filters.
    crop = st.multiselect('Select the crop type (optional)', ['sorghum', 'lettuce', 'maize'])
    
    # Add filters to the query.
    if crop:
        query['query']['bool']['must'].append({"terms": {"crop_type": crop}})
    
    # --- SPECIAL CASE: BOX AXIS ---
    if len(selected_columns) == 1 and selected_columns[0] == "box axis":
        # Ensure the coordinate fields exist.
        query['query']['bool']['must'].append({"exists": {"field": "nw_lat"}})
        query['query']['bool']['must'].append({"exists": {"field": "nw_lon"}})
        query['query']['bool']['must'].append({"exists": {"field": "se_lat"}})
        query['query']['bool']['must'].append({"exists": {"field": "se_lon"}})
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
                            "_source": {"includes": ["nw_lat", "nw_lon", "se_lat", "se_lon"]},
                            "size": 1
                        }
                    }
                }
            }
        }
        response = client.search(index=index_name, body=query)
        data = []
        for bucket in response['aggregations']['by_scan_date']['buckets']:
            row = {'scan_date': bucket['key_as_string'], 'boxes': []}
            for hit in bucket['box_axis']['hits']['hits']:
                row['boxes'].append(hit['_source'])
            data.append(row)
        df = pd.DataFrame(data).dropna()
        
        # Map visualization: let the user choose a scan date.
        date_choice = st.selectbox(
            'Select a date to visualize the boxes',
            df[df['boxes'].apply(lambda x: len(x) > 0)]['scan_date'].unique()
        )
        # Prepare a new query to get the boxes for the chosen date.
        query['query']['bool']['must'].append({"exists": {"field": "nw_lat"}})
        query['query']['bool']['must'].append({"exists": {"field": "nw_lon"}})
        query['query']['bool']['must'].append({"exists": {"field": "se_lat"}})
        query['query']['bool']['must'].append({"exists": {"field": "se_lon"}})
        date_choice_formatted = pd.to_datetime(date_choice).strftime('%Y%m%dT%H%M%S.%f') + '-0700'
        query['query']['bool']['must'].append({"term": {"scan_date": date_choice_formatted}})
        query['size'] = 10000
        if "aggs" in query:
            del query['aggs']
        response = client.search(index=index_name, body=query)
        
        data = []
        for hit in response['hits']['hits']:
            data.append(hit['_source'])
        df_boxes = pd.DataFrame(data)
        if len(df_boxes) > 1000:
            df_boxes = df_boxes.sample(n=1000)
        
        lon, lat = [], []
        min_nw_lon = math.inf
        max_se_lon = -math.inf
        min_se_lat = math.inf
        max_nw_lat = -math.inf
        for _, box in df_boxes.iterrows():
            lon.extend([box['nw_lon'], box['se_lon'], box['se_lon'], box['nw_lon'], None])
            lat.extend([box['nw_lat'], box['nw_lat'], box['se_lat'], box['se_lat'], None])
            min_nw_lon = min(min_nw_lon, box['nw_lon'])
            max_se_lon = max(max_se_lon, box['se_lon'])
            min_se_lat = min(min_se_lat, box['se_lat'])
            max_nw_lat = max(max_nw_lat, box['nw_lat'])
        zoom_lon = max_se_lon - min_nw_lon
        zoom_lat = max_nw_lat - min_se_lat
        zoom = -1.446 * max(zoom_lon, zoom_lat) + 8.5
        
        fig = go.Figure(go.Scattermapbox(
            mode="lines",
            fill="toself",
            lon=lon,
            lat=lat
        ))
        fig.update_layout(
            mapbox={
                'style': "open-street-map",
                "center": {"lon": (min_nw_lon + max_se_lon) / 2,
                           "lat": (min_se_lat + max_nw_lat) / 2},
                "zoom": zoom
            },
            showlegend=False,
            margin={"l": 0, "r": 0, "t": 0, "b": 0},
        )
    
    # --- GENERAL CASE: ONE OR MORE (non–box axis) COLUMNS ---
    else:
        # For each column (which is not box axis), run its query separately.
        results = {}  # dict to hold DataFrames keyed by column name.
        for col in selected_columns:
            # Create a deep copy of the query so that changes for one column
            # do not affect the queries for others.
            q = copy.deepcopy(query)
            # Use the same query modifications as already in your code.
            if col.startswith("azmet_"):
                q['aggs'] = {
                    "by_scan_date": {
                        "date_histogram": {
                            "field": "scan_date",
                            "calendar_interval": "day",
                            "format": "yyyy-MM-dd"
                        },
                        "aggs": {
                            "value": {
                                "top_hits": {
                                    "_source": {"includes": [col]},
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            else:
                q['aggs'] = {
                    "by_scan_date": {
                        "date_histogram": {
                            "field": "scan_date",
                            "calendar_interval": "day",
                            "format": "yyyy-MM-dd"
                        },
                        "aggs": {
                            "mean": {"avg": {"field": col}},
                            "median": {"percentiles": {"field": col, "percents": [50]}},
                            "max": {"max": {"field": col}},
                            "min": {"min": {"field": col}}
                        }
                    }
                }
            q["size"] = 0
            response = client.search(index=index_name, body=q)
            data = []
            if col.startswith("azmet_"):
                for bucket in response['aggregations']['by_scan_date']['buckets']:
                    row = {'scan_date': bucket['key_as_string']}
                    hits = bucket['value']['hits']['hits']
                    if hits:
                        row[col] = hits[0]['_source'][col]
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
            df = pd.DataFrame(data).dropna()
            results[col] = df
        
        # Create subplots so that each column is shown in its own panel,
        # with the x-axis (scan_date) shared across all panels.
        n_rows = len(selected_columns)
        if n_rows <= 0:
            st.warning("Please select at least one column to visualize.")
            return

        fig = make_subplots(rows=n_rows, cols=1, shared_xaxes=True,
                            subplot_titles=selected_columns)
        for i, col in enumerate(selected_columns, start=1):
            df = results[col]
            # For a given column, choose which trace to plot.
            # Here, for normal columns we choose the "mean" value (but you can extend this if needed),
            # and for azmet_ columns we plot the value.
            if col.startswith("azmet_"):
                if graph_type == 'Line':
                    fig.add_trace(go.Scatter(x=df['scan_date'], y=df[col],
                                             mode='lines', name=col), row=i, col=1)
                elif graph_type == 'Scatter':
                    fig.add_trace(go.Scatter(x=df['scan_date'], y=df[col],
                                             mode='markers', name=col), row=i, col=1)
                else:
                    fig.add_trace(go.Box(x=df['scan_date'], y=df[col],
                                           name=col), row=i, col=1)
            else:
                # Plot only the "mean" value (or you could plot all four if desired).
                if graph_type == 'Line':
                    fig.add_trace(go.Scatter(x=df['scan_date'], y=df['mean'],
                                             mode='lines', name=f'{col} mean'), row=i, col=1)
                elif graph_type == 'Scatter':
                    fig.add_trace(go.Scatter(x=df['scan_date'], y=df['mean'],
                                             mode='markers', name=f'{col} mean'), row=i, col=1)
                else:
                    fig.add_trace(go.Box(x=df['scan_date'], y=df['mean'],
                                           name=f'{col} mean'), row=i, col=1)
        fig.update_layout(title_text='Value of parameters over time',
                          showlegend=True,
                          height=300 * n_rows)
    
    st.plotly_chart(fig)

def compare_axis(client, index_name, query, get_all_columns_func):
    """
    Compare the values of two columns over time.
    """
    st.subheader("Compare the values of two columns over time")
    
    # Build a list of visualizable columns and extend with any azmet_ columns found.
    visualizable_columns = ['roi_temp', 'bounding_area_m2', 'mean_tgi', 'q1_tgi', 'q3_tgi']
    all_columns = get_all_columns_func(client, index_name)
    azmet_columns = [col for col in all_columns if col.startswith('azmet_')]
    visualizable_columns.extend(azmet_columns)
    
    # Use a multiselect so the user can pick one or more columns.
    selected_columns = st.multiselect(
        'Select the columns to visualize', 
        visualizable_columns,
        default=[visualizable_columns[0], visualizable_columns[1]]
    )

    if len(selected_columns) < 2:
        st.warning("Please select at least two columns to compare.")
        return

    # Ask user to select the visualization type - scatterplot matrix or parallel coordinates plot
    plot_type = st.selectbox('Select the visualization type', ['Scatterplot Matrix', 'Parallel Coordinates Plot'])

    # Ensure the query uses the selected columns.
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
    for col in selected_columns:
        query['aggs']['by_scan_date']['aggs'][col] = {"avg": {"field": col}}
    
    query["size"] = 0
    response = client.search(index=index_name, body=query)
    
    data = []
    
    for bucket in response['aggregations']['by_scan_date']['buckets']:
        row = {'scan_date': bucket['key_as_string']}
        for col in selected_columns:
            row[col] = bucket[col]['value']
        data.append(row)
    
    df = pd.DataFrame(data)
    df['year'] = pd.to_datetime(df['scan_date']).dt.year

    # filter data by columns where both columns have values
    df = df.dropna(subset=selected_columns)

    # Convert the year to integer to avoid fractional years in the color legend
    df['year'] = df['year'].astype(int)
    # st.write(df)

    if plot_type == 'Scatterplot Matrix':
        # make a scale mapping the year to a color
        color_scale = px.colors.qualitative.Set1
        color_map = {str(year): color for year, color in zip(df['year'].unique(), color_scale)}
        
        # Build a line plot matrix comparing the columns (year can just be a color legend)
        fig = px.scatter_matrix(df, dimensions=selected_columns, color='year', color_discrete_map=color_map)
        
        # make the figure a bit larger, proportional to the number of columns
        fig.update_layout(width=400 + 100 * len(selected_columns), height=400 + 100 * len(selected_columns))
        # Ensure the color legend is discrete
        fig.update_traces(marker=dict(size=5))
        fig.update_layout(coloraxis_colorbar=dict(
            title="Year",
            tickvals=list(color_map.keys()),
            ticktext=list(color_map.keys())
        ))

        st.plotly_chart(fig, config={'displayModeBar': False})
        st.markdown(
            """
            <style>
            .element-container {
                display: flex;
                justify-content: center;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

    else:
        # For parallel coordinates, include year as one of the dimensions
        dimensions = ['year'] + selected_columns
        
        # Create an empty container for the plot
        plot_container = st.empty()
        
        # Initialize the session state for color dimension if it doesn't exist
        if 'color_dimension' not in st.session_state:
            st.session_state.color_dimension = 'year'
        
        # Add a row for displaying the current coloring dimension
        st.markdown(f"**Currently coloring by:** {st.session_state.color_dimension}")
        
        # Create column buttons for dimension selection
        st.markdown("**Click to color by:**")
        cols = st.columns(len(dimensions))
        
 
        # Create buttons for each dimension to allow switching the color dimension
        for i, dim in enumerate(dimensions):
            if cols[i].button(dim, key=f"color_by_{dim}"):
                st.session_state.color_dimension = dim
                st.rerun()

        # Get the current coloring dimension
        color_dim = st.session_state.color_dimension
        if color_dim not in dimensions:
            color_dim = dimensions[0]

        # Build a parallel coordinates plot including year as the first axis
        # and color according to the selected dimension
        fig = px.parallel_coordinates(
            df, 
            dimensions=dimensions,
            color=color_dim,
            color_continuous_scale=px.colors.sequential.Viridis if color_dim != 'year' else px.colors.qualitative.Set1
        )
        
        # Update layout to provide better interaction and increase margin between title and plot
        fig.update_layout(
            width=800, 
            height=400,
            # Add tooltips for better UX
            hovermode='closest',
            # Add note about interaction in the title and increase top margin
            margin=dict(t=80, b=20, l=50, r=50),  # Increased top margin to prevent overlap
            title={
                'text': f'Parallel Coordinates Plot (Colored by {color_dim})',
                'y':0.98,  # Moved title position up
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            }
        )
        
        # Update the plot
        plot_container.plotly_chart(fig, config={'displayModeBar': True})
        
        # Add explanatory text about the interactions
        st.markdown("""
        **Interactions with Parallel Coordinates Plot:**
        - **Drag the axis labels** to reorder the axes.
        - **Click on the buttons below the plot** to color by different dimensions.
        """)
        
        st.markdown(
            """
            <style>
            .element-container {
                display: flex;
                justify-content: center;
            }
            </style>
            """,
            unsafe_allow_html=True
        )