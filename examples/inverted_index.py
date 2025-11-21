"""
Inverted index MapReduce example.
Creates an index mapping each word to the documents/lines it appears in.
"""

import string


def map_function(key, value):
    """
    Map function: emit (word, document_id) for each word.

    Args:
        key: Line number (used as document ID)
        value: Text line

    Yields:
        (word, document_id) tuples
    """
    words = value.translate(str.maketrans('', '', string.punctuation)).split()

    for word in words:
        if word:
            yield (word.lower(), f"doc_{key}")


def reduce_function(key, values):
    """
    Reduce function: collect all document IDs for a word.

    Args:
        key: Word
        values: List of document IDs

    Yields:
        (word, comma-separated unique document IDs) tuple
    """
    # Remove duplicates and sort
    unique_docs = sorted(set(values))
    yield (key, ','.join(unique_docs))


def combiner_function(key, values):
    """
    Combiner function: remove local duplicates.

    Args:
        key: Word
        values: List of local document IDs

    Yields:
        (word, document_id) tuples for unique local docs
    """
    for doc in set(values):
        yield (key, doc)
