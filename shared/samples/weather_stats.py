"""
Sample weather statistics MapReduce job with combiner.

Computes average temperature, humidity, and pressure from CSV data.
Input format: temperature,humidity,pressure (one measurement per line)

Demonstrates combiner with aggregation that requires tracking multiple statistics.
"""

def map_fn(text):
    """
    Map function that processes weather data lines.

    Args:
        text: Input text containing CSV lines

    Returns:
        List of partitions with (measurement_type, value) pairs
    """
    lines = text.strip().split('\n')
    result = []

    for line in lines:
        line = line.strip()
        # Skip empty lines and header
        if not line or line.startswith('Temperature'):
            continue

        try:
            # Parse the CSV line
            temp, humid, press = map(float, line.split(','))

            # Emit values for each measurement type
            result.append(('temperature', temp))
            result.append(('humidity', humid))
            result.append(('pressure', press))
        except ValueError:
            # Skip malformed lines
            continue

    return [result]  # Single partition


def reduce_fn(key, values):
    """
    Reduce function that computes statistics for each measurement type.

    Args:
        key: Measurement type ('temperature', 'humidity', or 'pressure')
        values: List of measurements (floats or dicts from combiner)

    Returns:
        Dictionary with average, min, max, and count
    """
    # Handle both raw values and combiner output
    if values and isinstance(values[0], dict):
        # Values from combiner - need to aggregate stats
        total_sum = sum(v['sum'] for v in values)
        total_count = sum(v['count'] for v in values)
        min_val = min(v['min'] for v in values)
        max_val = max(v['max'] for v in values)
    else:
        # Raw values from map phase
        values_list = list(values)
        total_sum = sum(values_list)
        total_count = len(values_list)
        min_val = min(values_list)
        max_val = max(values_list)

    return {
        'average': total_sum / total_count if total_count > 0 else 0,
        'min': min_val,
        'max': max_val,
        'count': total_count
    }


def combine_fn(key, values):
    """
    OPTIONAL: Combiner function for local aggregation of statistics.

    For computing averages, we can't just average the values locally.
    Instead, we track sum, count, min, and max, which can be properly
    aggregated in the reduce phase.

    Args:
        key: Measurement type
        values: List of measurements from local map outputs

    Returns:
        Dictionary with sum, count, min, max for this partition
    """
    values_list = list(values)

    return {
        'sum': sum(values_list),
        'count': len(values_list),
        'min': min(values_list),
        'max': max(values_list)
    }