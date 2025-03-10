#!/bin/bash
set -e

CONFIG_FILE="/app/config.json"
LOG_FILE="/app/progress.log"



# If the configuration file does not exist, launch the configuration UI.

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Configuration file not found. Launching configuration UI..."
    # Start the Streamlit params app on CPU core 1.
    taskset -c 1 streamlit run app/params_app.py --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false &
    STREAMLIT_PID=$!
    
    # Wait until the config file is created.
    while [ ! -f "$CONFIG_FILE" ]; do
        sleep 2
    done
    
    echo "Configuration received. Continuing with setup..."
fi

# Redirect all subsequent output to a log file (and to stdout) so the params app can display progress.
exec > >(stdbuf -oL tee -a "$LOG_FILE") 2>&1

# Read configuration values from the JSON file.
IRODS_USER=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['IRODS_USER'])")
IRODS_PASSWORD=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['IRODS_PASSWORD'])")
ELASTIC_PASSWORD=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['ELASTIC_PASSWORD'])")
UPDATE_DATA=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['UPDATE_DATA'])")

# Create .env file using the configuration values.
echo "Creating .env file..."
echo "ELASTIC_HOST=${ELASTIC_HOST:-localhost}
ELASTIC_PORT=${ELASTIC_PORT:-9200}
ELASTIC_USER=${ELASTIC_USER:-admin}
ELASTIC_PASSWORD=${ELASTIC_PASSWORD}" > .env

# Set up iRODS environment file.
echo "Setting up iRODS environment file..."
mkdir -p /root/.irods
echo "{
    \"irods_host\": \"${IRODS_HOST:-data.cyverse.org}\",
    \"irods_port\": ${IRODS_PORT:-1247},
    \"irods_user_name\": \"${IRODS_USER}\",
    \"irods_zone_name\": \"${IRODS_ZONE:-iplant}\",
    \"irods_home\": \"/${IRODS_ZONE:-iplant}/home/${IRODS_USER}\"
}" > /root/.irods/irods_environment.json

# Create iRODS authentication file.
echo "Initializing iRODS..."
echo "${IRODS_PASSWORD}" | iinit

# Function to wait for OpenSearch.
wait_for_opensearch() {
    echo "Waiting for OpenSearch..."
    while ! curl -s "http://localhost:9200" > /dev/null; do
        sleep 1
    done
    echo "OpenSearch is ready!"
}

# Start OpenSearch as the opensearch user in the background.
echo "Starting OpenSearch..."
su opensearch -c "./opensearch-2.17.0/bin/opensearch" > /app/opensearch_output.log 2>&1 &
OPENSEARCH_PID=$!

# Wait for OpenSearch to be ready.
wait_for_opensearch

# Check if data update is required.
if [ "${UPDATE_DATA,,}" == "true" ]; then
    # Run data preparation scripts for Season 11.
    echo "Preparing Season 11 data..."
    python3 data_preparation/scanner3D.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/Gantry_fieldbook_Aug-2020_Revised_Irr_TRT.csv" "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_2/scanner3DTop/"
    python3 data_preparation/flirIRCamera.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_3/flirIrCamera/s11_clustered_flir_identifications.csv"
    python3 data_preparation/stereoTop.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_3/stereoTop/season_11_clustering.csv"

    # Run data preparation scripts for Season 14.
    echo "Preparing Season 14 data..."
    python3 data_preparation/scanner3D.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/North_gantry_fieldbook_2022_replants.csv" "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/scanner3DTop/sorghum/"
    python3 data_preparation/drone.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/drone/sorghum/"
    python3 data_preparation/flirIRCamera.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/flirIrCamera/season_14_clustering_flir.csv"
    python3 data_preparation/stereoTop.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/stereoTop/season_14_clustering.csv"
    
    echo "Data preparation complete!"
    
fi

echo "Uploading data to OpenSearch..."
python3 search_configuration/upload_data.py

echo "Data upload complete!"
echo "term_loop"


# # Just before starting the main app, shut down the params app if it is still running.
# if ps -p $STREAMLIT_PID > /dev/null 2>&1; then
#     echo "Closing configuration UI..."
#     kill $STREAMLIT_PID
# fi

# echo "Starting main frontend app..."
# streamlit run app/main.py --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false

# Wait for all background processes to finish.
wait