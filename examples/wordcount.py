"""
Classic MapReduce word count example.
Counts the frequency of each word in the input text.
"""

import string


def map_function(key, value):
    """
    Map function: emit (word, 1) for each word in the line.

    Args:
        key: Line number (unused)
        value: Text line

    Yields:
        (word, 1) tuples
    """
    # Remove punctuation and split into words
    words = value.translate(str.maketrans('', '', string.punctuation)).split()

    for word in words:
        if word:  # Skip empty strings
            yield (word.lower(), 1)


def reduce_function(key, values):
    """
    Reduce function: sum all counts for a word.

    Args:
        key: Word
        values: List of counts (all 1s from map)

    Yields:
        (word, total_count) tuple
    """
    yield (key, sum(values))


def combiner_function(key, values):
    """
    Combiner function: pre-aggregate counts locally (same as reduce).

    Args:
        key: Word
        values: List of local counts

    Yields:
        (word, local_sum) tuple
    """
    yield (key, sum(values))
