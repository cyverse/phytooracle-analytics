# PhytoOracle Data Analytics

## Overview

This project leverages OpenSearch to index, search and visualize data scanned by the Field Analyzer, a state-of-the-art outdoor plant phenotyping platform located at the Maricopa Agricultural Center (MAC), focusing on plant metrics across different growth stages.

By integrating visualization dashboards and advanced searching and indexing capabilities, the solution aims to enable researchers to interact with, navigate and analyze the complex data sets seamlessly.

## Components

1. [automation](automation): Contains scripts for automating repetitive tasks such as data preparation, uploading and indexing workflows.

1. [data_preparation](data_preparation): Contains scripts for preparing the data in JSON format to be indexed. This includes data extraction, transformation, and cleaning processes.

1. [deployment](deployment): Contains scripts for deploying the solution.

1. [sample_queries](sample_queries): Contains predefined queries in Query DSL format used to extract relevant responses from the indexed data.

1. [search_configuration](search_configuration): Contains scripts to interact with the OpenSearch Server.


## USAGE

- **Installing dependencies**: To install dependencies, run the following command:
    ```pip install -r requirements.txt```

- **OpenSearch Configuration**: Get an instance of OpenSearch/ElasticSearch set up and then refer to `example.env` to write an `.env` file with the necessary environment variables for our program to access OpenSearch.

- **Data Preparation**: To perform data preparation, please refer to the documentation provided in the [data_preparation](data_preparation) directory. The usage of data preparation is unique for each sensor type. All prepared data should be available in the `output/` directory. 

- **OpenSearch**: Refer to the documentation provided in [search_configuration](search_configuration) to populate the data in the OpenSearch index.

- **Visualization**: To visualize the data, run:
    ```
    streamlit run app/vis.py
    ```

## DOCKER SETUP

- The entire app can be set up using the `Dockerfile` provided. In order to build the application, you can use the following command
    ```
    docker build -t phytooracle .
    ```
- To run the application, you can use the following command
    ```
    docker run -p 8501:8501 phytooracle
    ```
- The OpenSearch server is already a  part of the docker image, and is set up automatically when the container is run. If you want to access the OpenSearch server separately, ensure that you expose the port `9200` when running the container. You can do this by adding `-p 9200:9200` to the `docker run` command. The OpenSearch server will be accessible at `http://localhost:9200`.

