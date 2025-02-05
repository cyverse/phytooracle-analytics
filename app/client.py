# client.py
from opensearchpy import OpenSearch

def create_opensearch_client(host, port, auth):
    """
    Create and return an OpenSearch client.
    """
    client = OpenSearch(
        hosts=[{'host': host, 'port': port, 'scheme': 'https'}],
        http_compress=True,  # enables gzip compression for request bodies
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    return client
