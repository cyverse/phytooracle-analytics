# Search Configuration

## Overview

This directory contains scripts and configurations essential for setting up, modifying and interacting with the OpenSearch environment. This includes index mappings, data indexing / uploading scripts etc.

## Uses

- **Index Mappings**: Index mappings corresponding to the JSON input data in OpenSearch.
- **Upload Scripts**: Scripts designed to automate the uploading and indexing of JSON data present in the `output/` directory.

## Usage

- **Upload Data**
    
    ```
    python3 search_configuration/upload_data.py
    ```

    Uploads all data available in `output/` directory to the `phytooracle-index` in Opensearch - uses the index mappings provided in `search_configuration/index_mappings.json`.

- **Delete Index**

    ```
    python3 search_configuration/delete_index.py <index_name>
    ```

    Deletes `index_name` from OpenSearch, if an index of such name exists.

- **Delete Data in Index**
    ```
    python3 search_configuration/delete_data_in_index.py
    ```

    Deletes all data from `phytooracle-index` while preserving the index itself.

- **Get index summary data**
    
    ```
    python3 search_configuration/check_data.py
    ```

    Checks if `phytooracle-index` exists and if it does, provides a summary of the data in the index.

**NOTE**: As you may have seen, a lot of information about the index is hardcoded. Future iterations would refer to the environment file for all details regarding the index, including `delete_index.py`.