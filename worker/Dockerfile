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
COPY worker/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy and compile protobuf schema
COPY mapreduce.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mapreduce.proto

# Copy worker source code
COPY worker/ .

# Start worker process
CMD ["python3", "worker_server.py"]
