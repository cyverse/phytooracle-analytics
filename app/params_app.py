import streamlit as st
import json
import os
import time

from dotenv import load_dotenv

load_dotenv()

# Check if the deployment is running in Streamlit Sharing



st.set_page_config(layout="wide")

placeholder = st.empty()

if os.getenv("DEPLOYMENT_STATUS") != "complete":

    with placeholder.container():
        while True:
            # # print all environment variables
            # for key, value in os.environ.items():
            #     print(f"{key}: {value}")

            st.title("Deployment Parameters & Log Viewer")

            # --- Configuration Section ---
            if "submitted" not in st.session_state:
                st.session_state.submitted = False

            # If the config file exists, then set the submitted flag to True
            if os.path.exists("/app/config.json"):
                st.session_state.submitted = True

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

                # Ask the user if they want to update the opensearch JSON data from iRODS
                st.markdown("""
                ### Update OpenSearch Data
                - If you want to update the OpenSearch data from iRODS, check the box below.
                - This will update the data in OpenSearch with the latest data from iRODS.
                - **This process may take a while depending on the amount of data.**
                """)
                update_data = st.checkbox("Update OpenSearch Data")
                
                if st.button("Submit"):
                    if not irods_user or not irods_password or not elastic_password:
                        st.error("Please fill in all fields.")
                    else:
                        config = {
                            "IRODS_USER": irods_user,
                            "IRODS_PASSWORD": irods_password,
                            "ELASTIC_PASSWORD": elastic_password,
                            "UPDATE_DATA": update_data
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
                lf_tail = tail(LOG_FILE)
                log_placeholder.text(lf_tail)
            with c2:
                st.subheader("OpenSearch Output")
                opensearch_placeholder = st.empty()
                opensearch_placeholder.text(tail(OPENSEARCH_LOG))
            
            # if the last line of the log file (after removing whitespace) is term_loop, then the deployment is complete
            if lf_tail.split()[-1].strip() == "term_loop":
                os.putenv("DEPLOYMENT_STATUS", "complete")
                st.success("Deployment complete! You can now access the app.")
                break

            # Auto-refresh the app every 2 seconds to simulate "tail -f".
            time.sleep(2)
            st.rerun()

# If the deployment is not in progress, continue with the app
from main import app
with placeholder.container():
    app()