import streamlit as st
import json
import os
import time

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

# Use a placeholder to continuously update the log tail.
log_placeholder = st.empty()

# Display the tail of the log file.
log_placeholder.text(tail(LOG_FILE, 20))

# Auto-refresh the app every 2 seconds to simulate "tail -f".
time.sleep(2)
st.rerun()
