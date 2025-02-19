# Use Ubuntu 20.04 as base image
FROM ubuntu:20.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install Python 3.11 and system dependencies
RUN apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-distutils \
    python3.11-venv \
    python3-pip \
    wget \
    gnupg2 \
    curl \
    gcc \
    libssl-dev

# Install iRODS iCommands
RUN echo "deb [arch=amd64] https://packages.irods.org/apt/ focal main" > /etc/apt/sources.list.d/renci-irods.list && \
    wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y irods-icommands && \
    rm -rf /var/lib/apt/lists/*

# Create opensearch user and group
RUN groupadd -g 1000 opensearch && \
    useradd -u 1000 -g opensearch opensearch

# Create necessary directories for iRODS
RUN mkdir -p /root/.irods

# Set up Python 3.11 as default and install pip
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/pip3 pip3 /usr/local/bin/pip3 1

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create necessary directories
RUN mkdir -p output

# Set up OpenSearch 2.19.0
RUN curl -L -O https://artifacts.opensearch.org/releases/bundle/opensearch/2.19.0/opensearch-2.19.0-linux-x64.tar.gz \
    && tar -xzf opensearch-2.19.0-linux-x64.tar.gz \
    && rm opensearch-2.19.0-linux-x64.tar.gz \
    && chown -R opensearch:opensearch opensearch-2.19.0

# Configure OpenSearch
RUN mkdir -p /app/opensearch-2.19.0/config/certs && \
    echo "plugins.security.ssl.http.enabled: false" >> /app/opensearch-2.19.0/config/opensearch.yml && \
    echo "plugins.security.disabled: true" >> /app/opensearch-2.19.0/config/opensearch.yml && \
    echo "network.host: 0.0.0.0" >> /app/opensearch-2.19.0/config/opensearch.yml && \
    echo "discovery.type: single-node" >> /app/opensearch-2.19.0/config/opensearch.yml && \
    echo "plugins.security.ssl.transport.enabled: false" >> /app/opensearch-2.19.0/config/opensearch.yml && \
    chown -R opensearch:opensearch /app/opensearch-2.19.0/config

# Script to wait for OpenSearch and initialize everything
COPY <<'EOF' /app/init.sh
#!/bin/bash
set -e

# Create .env file using runtime environment variables
echo "ELASTIC_HOST=${ELASTIC_HOST:-localhost}
ELASTIC_PORT=${ELASTIC_PORT:-9200}
ELASTIC_USER=${ELASTIC_USER:-admin}
ELASTIC_PASSWORD=${ELASTIC_PASSWORD}" > .env

# Set up iRODS environment file
echo "{
    \"irods_host\": \"${IRODS_HOST:-data.cyverse.org}\",
    \"irods_port\": ${IRODS_PORT:-1247},
    \"irods_user_name\": \"${IRODS_USER:-tanmayagrawal21}\",
    \"irods_zone_name\": \"${IRODS_ZONE:-iplant}\",
    \"irods_home\": \"/${IRODS_ZONE:-iplant}/home/${IRODS_USER}\"
}" > /root/.irods/irods_environment.json

# Create iRODS authentication file
echo "${IRODS_PASSWORD}" | iinit

# Function to wait for OpenSearch
wait_for_opensearch() {
    echo "Waiting for OpenSearch..."
    while ! curl -s "http://localhost:9200" > /dev/null; do
        sleep 1
    done
    echo "OpenSearch is ready!"
}

# Start OpenSearch as opensearch user in the background
su opensearch -c "./opensearch-2.19.0/bin/opensearch" &

# Wait for it to be ready
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


# Upload data to OpenSearch
echo "Uploading data to OpenSearch..."
python3 search_configuration/upload_data.py

# Start the frontend
echo "Starting frontend..."
streamlit run app/main.py --server.address 0.0.0.0
EOF

RUN chmod +x /app/init.sh && \
    # Ensure proper permissions for OpenSearch directories
    mkdir -p /app/opensearch-2.19.0/logs && \
    mkdir -p /app/opensearch-2.19.0/data && \
    chown -R opensearch:opensearch /app/opensearch-2.19.0 && \
    chmod -R 755 /app/opensearch-2.19.0

# Expose ports for OpenSearch and Streamlit
EXPOSE 9200 8400 8501

# Set non-sensitive environment variables with defaults
ENV ELASTIC_HOST=localhost \
    ELASTIC_PORT=9200 \
    ELASTIC_USER=admin

# Run the initialization script
CMD ["/app/init.sh"]