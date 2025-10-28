# MapReduce Architecture (Week 1)

## System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         Host Machine                            │
│                                                                 │
│  ┌──────────────┐                                              │
│  │   Client     │                                              │
│  │   (CLI)      │                                              │
│  └──────┬───────┘                                              │
│         │ gRPC (port 50051)                                    │
│         │                                                       │
│  ┌──────▼──────────────────────────────────────────────────┐  │
│  │              Docker Compose Network                     │  │
│  │                                                          │  │
│  │  ┌────────────────┐                                     │  │
│  │  │  Coordinator   │◄──── gRPC Heartbeats ───┐          │  │
│  │  │   (port 50051) │                          │          │  │
│  │  └────────┬───────┘                          │          │  │
│  │           │                                   │          │  │
│  │           │ gRPC Task Assignment              │          │  │
│  │           │                                   │          │  │
│  │  ┌────────▼─────┬──────────┬──────────┬─────▼────────┐ │  │
│  │  │   Worker-1   │ Worker-2 │ Worker-3 │   Worker-4   │ │  │
│  │  │ (port 50052) │  (50053) │  (50054) │   (50055)    │ │  │
│  │  │   1 CPU      │  1 CPU   │  1 CPU   │    1 CPU     │ │  │
│  │  └──────────────┴──────────┴──────────┴──────────────┘ │  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │               Shared Volume (/shared)                    │  │
│  │  ┌─────────┬──────────┬──────────────┬──────────┐       │  │
│  │  │  input/ │ output/  │ intermediate/│  jobs/   │       │  │
│  │  └─────────┴──────────┴──────────────┴──────────┘       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Communication Flow

### 1. Job Submission
```
Client → Coordinator: SubmitJob(JobRequest)
  ├─ input_path: /shared/input/data.txt
  ├─ output_path: /shared/output/result
  ├─ job_file_path: /shared/jobs/wordcount.py
  ├─ num_map_tasks: 8
  └─ num_reduce_tasks: 4

Coordinator → Client: JobResponse
  ├─ job_id: "550e8400-e29b-41d4-a716-446655440000"
  └─ status: "SUBMITTED"
```

### 2. Job Status Query
```
Client → Coordinator: GetJobStatus(job_id)

Coordinator → Client: JobStatusResponse
  ├─ job_id: "550e8400-e29b-41d4-a716-446655440000"
  ├─ status: "SUBMITTED"
  ├─ total_map_tasks: 8
  ├─ completed_map_tasks: 0
  ├─ total_reduce_tasks: 4
  └─ completed_reduce_tasks: 0
```

### 3. Worker Heartbeat (Background)
```
Worker → Coordinator: Heartbeat
  ├─ worker_id: "worker-1"
  ├─ status: "IDLE"
  └─ available_slots: 1

Coordinator → Worker: HeartbeatResponse
  └─ acknowledged: true
```

### 4. Task Assignment (Future - Week 2)
```
Coordinator → Worker: AssignTask
  ├─ task_id: "task-001"
  ├─ task_type: "MAP"
  ├─ job_id: "550e8400-e29b-41d4-a716-446655440000"
  ├─ input_path: "/shared/input/data.txt"
  ├─ output_path: "/shared/intermediate/job_xxx/map_001"
  └─ job_file_path: "/shared/jobs/wordcount.py"

Worker → Coordinator: TaskAck
  ├─ task_id: "task-001"
  └─ accepted: true
```

## Data Flow (Future - Week 2+)

```
Input Data
   │
   ▼
┌──────────────┐
│  Map Phase   │  Workers read input, run map_fn, partition outputs
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│  Intermediate    │  Partitioned by hash(key) % num_reduce
│  Storage         │  Format: pickle files
└──────┬───────────┘
       │
       ▼
┌──────────────┐
│ Shuffle Phase│  Workers read their partition from all map outputs
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Reduce Phase │  Workers run reduce_fn, write final output
└──────┬───────┘
       │
       ▼
 Output Data
 (part-r-00000, part-r-00001, ...)
```

## File Organization

### Proto Files
- `proto/coordinator.proto` - Coordinator service definition
- `proto/worker.proto` - Worker service definition

### Generated Code
- `src/coordinator_pb2.py` - Coordinator message classes
- `src/coordinator_pb2_grpc.py` - Coordinator service stubs
- `src/worker_pb2.py` - Worker message classes
- `src/worker_pb2_grpc.py` - Worker service stubs

### Implementation
- `src/coordinator/server.py` - Coordinator server
- `src/worker/server.py` - Worker server
- `src/client/client.py` - Client CLI

### Infrastructure
- `docker-compose.yml` - Container orchestration
- `Dockerfile.coordinator` - Coordinator image
- `Dockerfile.worker` - Worker image
- `requirements.txt` - Python dependencies

## Current Status (Week 1)

✅ Implemented:
- gRPC service definitions
- Coordinator server (job submission, status, listing)
- Worker servers (heartbeat, task assignment handlers)
- Client CLI (all commands)
- Docker Compose setup
- Shared storage structure
- Integration tests

⏳ Not Yet Implemented (Future Weeks):
- Actual task execution logic
- Map/reduce function loading and execution
- Data partitioning and shuffling
- Combiner support
- Fault tolerance and retry logic
