import os

from elasticsearch import Elasticsearch
from loguru import logger


def get_elastic_client(
    url: str,
    api_id: str = os.environ.get("ELASTIC_API_ID"),
    api_key: str = os.environ.get("ELASTIC_API_KEY"),
    no_creds: bool = False,
    **kwargs
):
    """
    Returns an Elasticsearch client using api credentials.

    If api id and the api key are not provided, it looks for `ELASTIC_API_ID` and `ELASTIC_API_KEY` environment variables.

    Parameters
    ----------
    url : str
        Host of the Elasticsearch instance.
    api_id : str, optional
        Elastic API ID, by default os.environ.get("ELASTIC_API_ID")
    api_key : str, optional
        Elastic API Key, by default os.environ.get("ELASTIC_API_KEY")
    no_creds : bool, optional
        If True, will connect to an Elasticsearch instance with no creds.
        This is useful for connecting to a local instance of Elasticsearch.

    Returns
    -------
    Elasticsearch
        Elasticsearch client.

    Examples
    --------
    >>> from sidhulabs.elastic.client import get_elastic_client
    >>> es_client = get_elastic_client("https://elastic.sidhulabs.ca:443")
    """

    # For test clusters / local instances of Elasticsearch spun up
    # with a docker container.
    if no_creds:
        return Elasticsearch(hosts=[url], **kwargs)

    assert api_id is not None, "Pass in Elastic API ID to function or set env var ELASTIC_API_ID"
    assert api_key is not None, "Pass in Elastic API KEY to function or set env var ELASTIC_API_KEY"

    return Elasticsearch(hosts=[url], api_key=(api_id, api_key), **kwargs)


def test_connection(es_client):
    """
    Test connection to Elasticsearch instance.

    Parameters
    ----------
    es_client : Elasticsearch
        Elasticsearch client.

    Returns
    -------
    bool
        True if connection is successful.
    """
    try:
        es_client.info()
        return True
    except Exception as e:
        logger.error(e)
        return False
