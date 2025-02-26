#!/bin/bash
set -e

# If the configuration file does not exist, start the Streamlit configuration app.
if [ ! -f /app/config.json ]; then
    echo "Configuration file not found. Launching configuration UI..."
    # Start the Streamlit app in the background on a different CPU core
    taskset -c 1 streamlit run app/params_app.py --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false &
    STREAMLIT_PID=$!
    
    # Wait until the config file is created.
    while [ ! -f /app/config.json ]; do
        sleep 2
    done
    
    echo "Configuration received. Stopping configuration UI."
    kill $STREAMLIT_PID
fi

# Read configuration values from the JSON file.
IRODS_USER=$(python3 -c "import json; print(json.load(open('/app/config.json'))['IRODS_USER'])")
IRODS_PASSWORD=$(python3 -c "import json; print(json.load(open('/app/config.json'))['IRODS_PASSWORD'])")
ELASTIC_PASSWORD=$(python3 -c "import json; print(json.load(open('/app/config.json'))['ELASTIC_PASSWORD'])")

# Create .env file using the configuration values.
echo "ELASTIC_HOST=${ELASTIC_HOST:-localhost}
ELASTIC_PORT=${ELASTIC_PORT:-9200}
ELASTIC_USER=${ELASTIC_USER:-admin}
ELASTIC_PASSWORD=${ELASTIC_PASSWORD}" > .env

# Set up iRODS environment file.
echo "{
    \"irods_host\": \"${IRODS_HOST:-data.cyverse.org}\",
    \"irods_port\": ${IRODS_PORT:-1247},
    \"irods_user_name\": \"${IRODS_USER}\",
    \"irods_zone_name\": \"${IRODS_ZONE:-iplant}\",
    \"irods_home\": \"/${IRODS_ZONE:-iplant}/home/${IRODS_USER}\"
}" > /root/.irods/irods_environment.json

# Create iRODS authentication file.
echo "${IRODS_PASSWORD}" | iinit

# (Continue with waiting for OpenSearch, running data preparation scripts, uploading data, etc.)

# Function to wait for OpenSearch
wait_for_opensearch() {
    echo "Waiting for OpenSearch..."
    while ! curl -s "http://localhost:9200" > /dev/null; do
        sleep 1
    done
    echo "OpenSearch is ready!"
}


# Start OpenSearch as the opensearch user in the background.
su opensearch -c "./opensearch-2.19.0/bin/opensearch" &

# Wait for OpenSearch to be ready.
wait_for_opensearch

# Run data preparation scripts
echo "Preparing data..."
# Season 11
python3 data_preparation/scanner3D.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/Gantry_fieldbook_Aug-2020_Revised_Irr_TRT.csv" "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_2/scanner3DTop/"
python3 data_preparation/flirIRCamera.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_3/flirIrCamera/s11_clustered_flir_identifications.csv"
python3 data_preparation/stereoTop.py "/iplant/home/shared/phytooracle/season_11_sorghum_yr_2020/level_3/stereoTop/season_11_clustering.csv"

# Season 14
python3 data_preparation/scanner3D.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/North_gantry_fieldbook_2022_replants.csv" "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/scanner3DTop/sorghum/"
python3 data_preparation/drone.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/drone/sorghum/"
python3 data_preparation/flirIRCamera.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/flirIrCamera/season_14_clustering_flir.csv"
python3 data_preparation/stereoTop.py "/iplant/home/shared/phytooracle/season_14_sorghum_yr_2022/level_2/stereoTop/season_14_clustering.csv"



echo "Uploading data to OpenSearch..."
python3 search_configuration/upload_data.py

echo "Starting frontend..."
streamlit run app/main.py --server.address 0.0.0.0 --server.enableCORS false --server.enableXsrfProtection false
