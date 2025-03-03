import streamlit as st
import json
import os
import time

from dotenv import load_dotenv

load_dotenv()

# Check if the deployment is running in Streamlit Sharing

while os.environ.get("DEPLOYMENT_IN_PROGRESS", False):
    st.title("Deployment Parameters & Log Viewer")

    # --- Configuration Section ---
    if "submitted" not in st.session_state:
        st.session_state.submitted = False

    if not st.session_state.submitted:
        irods_user = st.text_input("DataStore Username")
        irods_password = st.text_input("DataStore Password", type="password")
        st.markdown("""
        ### OpenSearch Password Guidelines
        - Must be at least 8 characters long
        - Should include both uppercase and lowercase letters
        - Should contain at least one number
        - Should have at least one special character (e.g., @, #, $, etc.)
        
        **If you do not follow these guidelines, the deployment will fail.**
        """)
        elastic_password = st.text_input("OpenSearch Password", type="password")
        
        if st.button("Submit"):
            if not irods_user or not irods_password or not elastic_password:
                st.error("Please fill in all fields.")
            else:
                config = {
                    "IRODS_USER": irods_user,
                    "IRODS_PASSWORD": irods_password,
                    "ELASTIC_PASSWORD": elastic_password
                }
                with open("/app/config.json", "w") as f:
                    json.dump(config, f)
                st.success("Parameters saved! The deployment will continue shortly.")
                st.session_state.submitted = True

    # --- Tail-like Log Viewer Section ---
    st.subheader("Deployment Log (Tail View)")

    LOG_FILE = "/app/progress.log"
    OPENSEARCH_LOG = "/app/opensearch_output.log"

    def tail(filepath, lines=20):
        """Return the last 'lines' of the file."""
        if not os.path.exists(filepath):
            return "Log file not available yet."
        try:
            with open(filepath, "r") as f:
                all_lines = f.readlines()
                return "".join(all_lines[-lines:])
        except Exception as e:
            return f"Error reading log: {e}"


    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Deployment Progress")
        log_placeholder = st.empty()
        log_placeholder.text(tail(LOG_FILE))
    with c2:
        st.subheader("OpenSearch Output")
        opensearch_placeholder = st.empty()
        opensearch_placeholder.text(tail(OPENSEARCH_LOG))

    # Auto-refresh the app every 2 seconds to simulate "tail -f".
    time.sleep(2)
    load_dotenv()
    st.rerun()

# If the deployment is not in progress, continue with the app
from main import app

app()