# PocketFlow Redis Example

This example demonstrates how to use PocketFlow with Redis for distributed job execution and shared data storage with job_id isolation.

## Features

- **Redis-backed shared store** with job_id isolation
- **Queue-based job execution** for distributed processing
- **Distributed worker processing** across multiple processes/machines
- **Job status tracking** and result retrieval
- **Both sync and async** node processing support

## Prerequisites

1. **Redis Server**: Install and run Redis on localhost:6379
   ```bash
   # Install Redis (Ubuntu/Debian)
   sudo apt-get install redis-server
   
   # Start Redis
   sudo systemctl start redis-server
   
   # Or run Redis in Docker
   docker run -d -p 6379:6379 redis:latest
   ```

2. **Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Architecture

The Redis-based PocketFlow architecture consists of:

```
[Job Creator] → [Redis Queue] → [Worker 1]
                              → [Worker 2]
                              → [Worker N]
                                    ↓
[Redis Shared Store (per job_id)]
```

### Components

1. **RedisSharedStore**: Job-isolated shared memory using Redis
2. **RedisQueueManager**: Manages job queue and results
3. **RedisWorker**: Processes jobs from the queue
4. **QueueMessage**: Structured message format with job_id context

## Usage

### Basic Example

```python
from pocketflow import Node, RedisSharedStore, RedisQueueManager, RedisWorker

# Define your nodes
class ProcessDataNode(Node):
    def prep(self, shared):
        return shared.get("input_data")
    
    def exec(self, data):
        return data.upper()
    
    def post(self, shared, prep_res, exec_res):
        shared["output"] = exec_res
        return "complete"

# Create worker registry
node_registry = {"ProcessDataNode": ProcessDataNode()}

# Create queue manager and worker
queue_manager = RedisQueueManager()
worker = RedisWorker(node_registry, queue_manager)

# Create a job
job_id = queue_manager.create_job("ProcessDataNode", {"input_data": "hello"})

# Process the job
message = queue_manager.dequeue()
worker.process_message(message)

# Get result
result = queue_manager.get_job_result(job_id)
```

### Distributed Workers

Run multiple workers across different processes:

```bash
# Terminal 1 - Worker 1
python worker.py --worker-id 1

# Terminal 2 - Worker 2  
python worker.py --worker-id 2

# Terminal 3 - Job Creator
python job_creator.py
```

### Job Isolation

Each job gets its own isolated shared store:

```python
job1_id = queue_manager.create_job("ProcessDataNode", {"data": "job1"})
job2_id = queue_manager.create_job("ProcessDataNode", {"data": "job2"})

# Each job has isolated storage
shared1 = RedisSharedStore(job1_id)
shared2 = RedisSharedStore(job2_id)

shared1["result"] = "job1 result"
shared2["result"] = "job2 result"

# Data is completely isolated
assert shared1["result"] != shared2["result"]
```

## Running the Example

1. **Start Redis**:
   ```bash
   redis-server
   ```

2. **Run the demo**:
   ```bash
   cd cookbook/pocketflow-redis-example
   python main.py
   ```

3. **Run distributed workers** (optional):
   ```bash
   python worker.py --worker-id 1 &
   python worker.py --worker-id 2 &
   python job_creator.py
   ```

## Key Benefits

1. **Scalability**: Add more workers to handle increased load
2. **Fault Tolerance**: Jobs persist in Redis even if workers fail
3. **Isolation**: Each job has completely isolated data storage
4. **Monitoring**: Track job status and worker performance
5. **Flexibility**: Mix sync and async nodes in the same flow

## Message Format

Queue messages follow this structure:

```json
{
  "job_id": "abc123",
  "node_name": "ProcessDataNode",
  "action": "default",
  "params": {"key": "value"},
  "timestamp": 1234567890.123
}
```

## Production Considerations

- Use Redis clustering for high availability
- Implement proper error handling and retries
- Monitor queue length and worker performance
- Set appropriate TTL for job data
- Use Redis Streams for more advanced queue features
- Implement job priorities if needed

## Troubleshooting

1. **Redis Connection Issues**:
   - Verify Redis is running: `redis-cli ping`
   - Check Redis logs: `sudo journalctl -u redis`

2. **Job Stuck in Queue**:
   - Check worker logs for errors
   - Verify node registry contains required nodes
   - Monitor queue length: `redis-cli LLEN pf:queue`

3. **Memory Issues**:
   - Implement TTL for job data
   - Clean up completed jobs regularly
   - Monitor Redis memory usage