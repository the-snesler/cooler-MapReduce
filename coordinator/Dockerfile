FROM python:3.11

# Set working directory
WORKDIR /app

# Install system dependencies needed for grpcio compilation
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy coordinator source code
COPY . .

# Expose gRPC port
EXPOSE 50051

# Start coordinator server
CMD ["python3", "coordinator_server.py"]
