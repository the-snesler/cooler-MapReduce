# Cooler MapReduce

A simplified implementation of Google's MapReduce framework optimized for deployment on a single VM with Docker containers.

## Architecture

- **Coordinator**: Manages job submission and task distribution
- **Workers**: Execute map and reduce tasks (4 workers, each limited to 1 CPU)
- **Shared Storage**: Common volume mounted to all containers
- **Client**: Command-line tool for job submission and monitoring

## Prerequisites

- Docker and Docker Compose
- Python 3.9+

## Quick Start

### 1. Build and Start the Cluster

```bash
docker-compose up --build
```

This will start:
- 1 Coordinator container (port 50051)
- 4 Worker containers

### 2. Submit a Job

From the host machine:

```bash
python src/client/client.py submit \
  --input /shared/input/wordcount_input.txt \
  --output /shared/output/wordcount_result \
  --job-file /shared/jobs/wordcount.py \
  --num-map 8 \
  --num-reduce 4
```

### 3. Check Job Status

```bash
python src/client/client.py status --job-id <job_id>
```

### 4. List All Jobs

```bash
python src/client/client.py list
```

### 5. Get Job Results

```bash
python src/client/client.py results --job-id <job_id>
```

## Development Setup

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Generate gRPC Code

```bash
python -m grpc_tools.protoc -I./proto --python_out=./src --grpc_python_out=./src proto/coordinator.proto proto/worker.proto
```

### Run Coordinator Locally

```bash
python src/coordinator/server.py
```

### Run Worker Locally

```bash
python src/worker/server.py --worker-id worker-1 --coordinator localhost:50051 --port 50052
```

### Run Client Locally

```bash
python src/client/client.py --coordinator-host localhost:50051 <command>
```

## Project Structure

```
.
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile.coordinator      # Coordinator container image
├── Dockerfile.worker          # Worker container image
├── requirements.txt           # Python dependencies
├── proto/                     # gRPC protocol definitions
│   ├── coordinator.proto
│   └── worker.proto
├── src/
│   ├── coordinator/          # Coordinator implementation
│   │   └── server.py
│   ├── worker/              # Worker implementation
│   │   └── server.py
│   └── client/              # Client CLI
│       └── client.py
├── shared/                   # Shared storage volume
│   ├── input/               # Input data files
│   ├── output/              # Final output files
│   ├── intermediate/        # Intermediate map outputs
│   └── jobs/                # User job files
├── tests/                   # Test suite
└── examples/                # Example applications
```

## Implementation Status

### Week 1: Core Infrastructure ✅
- [x] Docker Compose setup
- [x] gRPC service definitions
- [x] Basic coordinator scaffolding
- [x] Basic worker scaffolding
- [x] Shared storage setup

### Week 2: MapReduce Core ✅
- [x] Task scheduling and distribution
- [x] Map task execution
- [x] Reduce task execution
- [x] Data partitioning and shuffling

### Week 3: Combiner Support (Coming Soon)
- [ ] Combiner integration in map tasks
- [ ] Testing and validation

### Week 4: Testing & Evaluation (Coming Soon)
- [ ] Example applications
- [ ] Performance benchmarking
- [ ] Documentation and report

## License

This is an educational project for learning MapReduce concepts.
