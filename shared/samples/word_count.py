"""
Sample word count MapReduce job.
"""

def map_fn(key, value, output):
    """Map function that splits text into words and emits (word, 1) pairs."""
    # Split the text into words
    words = value.strip().lower().split()
    
    # Emit each word with count 1
    for word in words:
        # Remove any punctuation
        word = word.strip('.,!?')
        if word:
            output.emit(word, 1)

def reduce_fn(key, values, output):
    """Reduce function that sums up the counts for each word."""
    # Sum all counts for this word
    total = sum(values)
    # Emit final count for the word
    output.emit(key, total)