# filters.py
import datetime
import streamlit as st

def render_filters():
    """
    Render and return the filter options from the Streamlit sidebar.
    """
    st.sidebar.title('Filters')
    
    crop_type = st.sidebar.multiselect('Crop Type', ['sorghum', 'lettuce', 'maize'])
    scan_date_from = st.sidebar.date_input('Scan Date From', value=datetime.date(2018, 1, 1))
    scan_date_to = st.sidebar.date_input('Scan Date To', value=datetime.date(2030, 12, 31))
    sensor_type = st.sidebar.multiselect('Sensor Type', ['flirIrCamera', 'scanner3DTop', 'drone', 'stereoTop'])
    year = st.sidebar.multiselect('Year', ['2022', '2021', '2020', '2019', '2018'])
    
    return crop_type, scan_date_from, scan_date_to, sensor_type, year
