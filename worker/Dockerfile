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

# Copy worker source code
COPY . .

# Start worker process
CMD ["python3", "worker_server.py"]
