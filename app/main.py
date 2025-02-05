# app.py
import streamlit as st
from client import create_opensearch_client
from config import ELASTIC_HOST, ELASTIC_PORT, ELASTIC_USER, ELASTIC_PASSWORD, INDEX_NAME
from filters import render_filters
from data import get_data, get_all_columns
from visualizations import (
    get_scan_count,
    get_vis,
    get_comparison_vis,
    # get_vis_over_time,  # Uncomment if you want to include this visualization.
    visualize_parameters
)

def app():
    # Use session state to control one-time initializations
    if 'first_time' not in st.session_state:
        st.session_state.first_time = True

    st.set_page_config(page_title="PhytoOracle Analytics", layout="wide")
    st.title("PhytoOracle Analytics")
    
    # Render the sidebar filters and retrieve filter values
    crop_type, from_date, to_date, sensor_type, year = render_filters()

    # Connect to OpenSearch
    auth = (ELASTIC_USER, ELASTIC_PASSWORD)
    client = create_opensearch_client(ELASTIC_HOST, ELASTIC_PORT, auth)
    
    try:
        if st.session_state.first_time:
            # Build the query and show a data overview
            query = get_data(client, crop_type, from_date, to_date, sensor_type, year, INDEX_NAME)
            # Call various visualizations
            col1, col2 = st.columns(2, gap="medium")
            col3, col4 = st.columns(2, gap="medium")
            with col1:
                get_scan_count(client, INDEX_NAME, query)
            with col2:
                get_vis(client, INDEX_NAME, query)
            with col3:
                get_comparison_vis(client, INDEX_NAME, query)
            with col4:
                visualize_parameters(client, INDEX_NAME, query, get_all_columns)
    except Exception as e:
        st.warning("Either the data is not available or there was an error processing the data.")
        st.write(e)

if __name__ == "__main__":
    app()
