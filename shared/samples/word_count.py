"""
Sample word count MapReduce job with optional combiner.

This job counts word frequencies in input text.
Demonstrates how combiner optimization reduces intermediate data.
"""

def map_fn(text):
    """
    Map function that splits text into words and emits (word, 1) pairs.

    Args:
        text: Input text string

    Returns:
        List of partitions, where each partition is a list of (key, value) tuples.
        For this simple example, returns a single partition.
    """
    words = text.strip().lower().split()

    result = []
    for word in words:
        # Remove any punctuation
        word = word.strip('.,!?;:"\'-')
        if word:  # Skip empty strings
            result.append((word, 1))

    return [result]  # Single partition


def reduce_fn(key, values):
    """
    Reduce function that sums up the counts for each word.

    Args:
        key: The word
        values: List of counts (all 1s from map phase, or combined counts)

    Returns:
        Total count for this word
    """
    return sum(values)


def combine_fn(key, values):
    """
    OPTIONAL: Combiner function for local aggregation of word counts.

    This reduces intermediate data size significantly by pre-aggregating
    counts locally before the shuffle phase. For word count, this is safe
    because addition is associative and commutative.

    Args:
        key: The word
        values: List of counts (all 1s from map phase)

    Returns:
        Combined count for this word in this partition
    """
    return sum(values)