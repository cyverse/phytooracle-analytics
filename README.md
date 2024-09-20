# PhytoOracle Data Analytics Solution

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
- **Coming soon**

## NOTE

- Please use `requirements_temp.txt` for now.


## TODO

- `data_preparation/data_preparation.py`: Remove `Pandas` dependency - should be doable with simple csv_reader. 
- Remove `myenv` from `.gitignore` - too specific for one user (Tanmay Agrawal)