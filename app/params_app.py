import streamlit as st
import json
import os

st.title("Deployment Parameters")

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
        st.success("Parameters saved! You may now close this page.")
