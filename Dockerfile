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
    libssl-dev \
    openssl

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

# Set up OpenSearch 2.17.0
RUN curl -L -O https://artifacts.opensearch.org/releases/bundle/opensearch/2.17.0/opensearch-2.17.0-linux-x64.tar.gz \
    && tar -xzf opensearch-2.17.0-linux-x64.tar.gz \
    && rm opensearch-2.17.0-linux-x64.tar.gz \
    && chown -R opensearch:opensearch opensearch-2.17.0

# Configure OpenSearch
RUN mkdir -p /app/opensearch-2.17.0/config/certs && \
    echo "plugins.security.ssl.http.enabled: false" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "plugins.security.disabled: true" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "network.host: 0.0.0.0" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "discovery.type: single-node" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "plugins.security.ssl.transport.enabled: false" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "action.auto_create_index: true" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    echo "cluster.blocks.read_only: false" >> /app/opensearch-2.17.0/config/opensearch.yml && \
    chown -R opensearch:opensearch /app/opensearch-2.17.0/config

RUN chmod +x /app/init.sh && \
    # Ensure proper permissions for OpenSearch directories
    mkdir -p /app/opensearch-2.17.0/logs && \
    mkdir -p /app/opensearch-2.17.0/data && \
    chown -R opensearch:opensearch /app/opensearch-2.17.0 && \
    chmod -R 755 /app/opensearch-2.17.0

# Generate a random password that meets the requirements
RUN mkdir -p /app/secrets && \
    # Generate random password with at least one uppercase, lowercase, number, and special character
    openssl rand -base64 12 | tr -d '\n' > /tmp/random_string && \
    (cat /tmp/random_string; echo -n 'A1@'; cat /dev/urandom | tr -dc 'a-zA-Z0-9@#$%^&*()_+' | head -c 4) > /app/secrets/opensearch_password && \
    chmod 600 /app/secrets/opensearch_password && \
    rm /tmp/random_string

# Start OpenSearch temporarily, populate index, and then shut it down
RUN mkdir -p /app/data_prep_logs && \
    su opensearch -c "./opensearch-2.17.0/bin/opensearch" > /app/data_prep_logs/opensearch.log 2>&1 & \
    echo "Waiting for OpenSearch to start..." && \
    until curl -s http://localhost:9200 > /dev/null; do sleep 2; done && \
    echo "OpenSearch started, populating index..." && \
    # Get the generated password
    OPENSEARCH_PASSWORD=$(cat /app/secrets/opensearch_password) && \
    # Create a default configuration for initial data load
    echo "{\"IRODS_USER\": \"anonymous\", \"IRODS_PASSWORD\": \"\", \"ELASTIC_PASSWORD\": \"$OPENSEARCH_PASSWORD\", \"UPDATE_DATA\": \"false\"}" > /app/config.json && \
    # Set environment variables for upload script
    ELASTIC_HOST=localhost ELASTIC_PORT=9200 ELASTIC_USER=admin ELASTIC_PASSWORD=$OPENSEARCH_PASSWORD \
    python3 search_configuration/upload_data.py 2>&1 | tee /app/data_prep_logs/upload.log && \
    echo "Initial data upload complete, stopping OpenSearch..." && \
    pkill -f opensearch && \
    echo "Initial OpenSearch index built and populated" && \
    # Remove the config.json but keep the password file for future use
    rm /app/config.json

# Expose ports for OpenSearch and Streamlit
EXPOSE 9200 8400 8501

# Set non-sensitive environment variables with defaults
ENV ELASTIC_HOST=localhost \
    ELASTIC_PORT=9200 \
    ELASTIC_USER=admin

# Run the initialization script
CMD ["/app/init.sh"]