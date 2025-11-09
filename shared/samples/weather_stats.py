"""
Sample weather statistics MapReduce job.
Computes average temperature, humidity, and pressure by time period.
"""

def map_fn(key, value, output):
    """Map function that processes weather data lines."""
    # Skip header line
    if value.startswith('Temperature'):
        return
        
    # Parse the CSV line
    temp, humid, press = map(float, value.strip().split(','))
    
    # Emit values for each measurement type
    output.emit('temperature', temp)
    output.emit('humidity', humid)
    output.emit('pressure', press)

def reduce_fn(key, values, output):
    """Reduce function that computes average for each measurement type."""
    values = list(values)  # Convert iterator to list to use it multiple times
    
    # Compute average
    avg = sum(values) / len(values)
    
    # Emit results with measurement type and statistics
    output.emit(key, {
        'average': avg,
        'min': min(values),
        'max': max(values),
        'count': len(values)
    })