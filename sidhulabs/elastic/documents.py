from __future__ import annotations

from collections import deque
from typing import Any, Dict, Iterable, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan


def get_all(es_client: Elasticsearch, index: str, **kwargs) -> Iterable[Dict[str, Any]]:
    """
    Get all documents from an index.

    Parameters
    ----------
    es_client : Elasticsearch
        Elasticsearch client.
    index : str
        Elasticsearch index
    **kwargs:
        Keyword arguments to pass to `scan` from elasticsearch.helpers.

    Returns
    -------
    List[Dict[str, Any]]
        List of documents.

    Examples
    --------
    >>> from sidhulabs.elastic.documents import get_all
    >>> es_client = get_elastic_client("https://elastic.sidhulabs.ca:443")
    >>> get_all(es_client, "test-index")
    """

    return scan(es_client, index=index, **kwargs)


def insert(es_client: Elasticsearch, index: str, docs: Dict[str, Any] | List[Dict[str, Any]], **kwargs) -> Any:
    """
    Inserts documents into an index.

    If passing in a List of documents, kwargs gets passed into `parallel_bulk` from elasticsearch.helpers.

    If passing in a a single document, kwargs gets passed into `index` from the elasticsearch client.

    Parameters
    ----------
    es_client : Elasticsearch
        Elasticsearch client.
    index : str
        Index name.
    docs : Dict[str, Any] or List[Dict[str, Any]]
        List of documents to insert.
    **kwargs
        Keyword arguments.

    Examples
    --------
    >>> from sidhulabs.elastic.documents import insert
    >>> es_client = get_elastic_client("https://elastic.sidhulabs.ca:443")
    >>> insert(es_client, "test-index", {"id": 1, "name": "John Doe"})
    >>> insert(es_client, "test-index", [{"id": 1, "name": "John Doe"}, {"id": 2, "name": "Jane Doe"}])
    """

    if isinstance(docs, list):
        resp = deque(parallel_bulk(client=es_client, actions=docs, index=index, **kwargs), maxlen=0)
    elif isinstance(docs, dict):
        resp = es_client.index(index=index, body=docs, **kwargs)
    else:
        raise ValueError("`docs` must be a list or a dict.")

    return resp


def delete(es_client: Elasticsearch, index: str, doc_ids: List[str | int] | str | int, **kwargs) -> Any:
    """
    Deletes a document from an index.

    Kwargs gets passed into the delete function of the Elasticsearch client.

    Parameters
    ----------
    es_client : Elasticsearch
        Elasticsearch client.
    index : str
        Index name.
    doc_id : str or int
        Document ID.
    doc : Dict[str, Any]
        Document to delete.
    **kwargs
        Keyword arguments to pass to delete function of Elasticsearch client.

    Examples
    --------
    >>> from sidhulabs.elastic.documents import delete
    >>> es_client = get_elastic_client("https://elastic.sidhulabs.ca:443")
    >>> delete(es_client=es_client, index="test-index", doc_ids=1)
    >>> delete(es_client=es_client, index="test-index", doc_ids=[1, 2])
    """

    if isinstance(doc_ids, list):
        actions = [{"_op_type": "delete", "_id": doc_id} for doc_id in doc_ids]
        resp = deque(parallel_bulk(client=es_client, actions=actions, index=index, **kwargs), maxlen=0)
    elif isinstance(doc_ids, (str, int)):
        resp = es_client.delete(index=index, id=doc_ids, **kwargs)
    else:
        raise ValueError("`doc_ids` must be a list, string, or integer.")

    return resp
