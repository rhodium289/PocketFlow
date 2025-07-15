# PocketFlow Redis System - Docker Setup

The easiest way to test the Redis-based PocketFlow system is using Docker Compose. This setup includes Redis, multiple workers, and automated testing.

## Quick Start

1. **Clone and navigate to the project**:
   ```bash
   git clone <repository-url>
   cd PocketFlow
   ```

2. **Start the entire system**:
   ```bash
   docker-compose up -d
   ```

3. **Watch the logs**:
   ```bash
   docker-compose logs -f
   ```

4. **Run the test suite**:
   ```bash
   docker-compose run test-runner
   ```

## Services Overview

The Docker Compose setup includes:

- **redis**: Redis server for queue and shared storage
- **worker1 & worker2**: Two distributed workers processing jobs
- **monitor**: Monitors queue size every 10 seconds
- **job-creator**: Container for manual job creation
- **test-runner**: Runs comprehensive system tests

## Testing Commands

### Run Automated Tests
```bash
# Full test suite
docker-compose run test-runner

# Individual test components
docker-compose exec redis redis-cli ping
docker-compose exec worker1 python -c "print('Worker 1 health check')"
```

### Manual Job Creation
```bash
# Create simple jobs
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --num-jobs 5

# Create batch jobs with monitoring
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --job-type batch --batch-size 3 --monitor

# Create retry jobs
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --job-type retry --num-jobs 2 --monitor
```

### Monitor the System
```bash
# Watch queue size
docker-compose logs -f monitor

# Check worker stats
docker-compose logs worker1 | grep "stats:"
docker-compose logs worker2 | grep "stats:"

# Redis queue inspection
docker-compose exec redis redis-cli llen pf:queue
docker-compose exec redis redis-cli keys "pf:*"
```

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   Job Creator   │    │     Monitor     │
└─────────┬───────┘    └─────────────────┘
          │                       │
          ▼                       ▼
┌─────────────────────────────────────────┐
│              Redis Queue                │
│         (pf:queue, pf:result:*)         │
└─────────┬───────────────────┬───────────┘
          │                   │
          ▼                   ▼
┌─────────────────┐    ┌─────────────────┐
│    Worker 1     │    │    Worker 2     │
│                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ LoadDataNode│ │    │ │ ProcessNode │ │
│ │ ProcessNode │ │    │ │ SaveDataNode│ │
│ │ SaveDataNode│ │    │ │ RetryNode   │ │
│ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘
          │                   │
          ▼                   ▼
┌─────────────────────────────────────────┐
│       Redis Shared Store               │
│     (pf:{job_id}:* namespaces)         │
└─────────────────────────────────────────┘
```

## Example Workflows

### 1. Basic Processing Flow
```bash
# Start system
docker-compose up -d

# Create a simple job
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --job-type simple --monitor

# Expected flow: LoadDataNode → ProcessDataNode → SaveDataNode
```

### 2. Batch Processing
```bash
# Create multiple jobs
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --job-type batch --batch-size 5 --monitor

# Watch workers process jobs in parallel
docker-compose logs -f worker1 worker2
```

### 3. Fault Tolerance Testing
```bash
# Create jobs
docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --num-jobs 10

# Stop one worker
docker-compose stop worker1

# Jobs continue processing on worker2
docker-compose logs -f worker2

# Restart worker1 - it will resume processing
docker-compose start worker1
```

## Validation Tests

The `test-runner` service runs comprehensive tests:

1. **Redis Connection**: Verifies Redis connectivity
2. **Queue Manager**: Tests job creation and queue operations
3. **Shared Store**: Validates job isolation and data operations
4. **Job Processing**: End-to-end job execution
5. **Multiple Jobs**: Concurrent job processing
6. **Job Isolation**: Data isolation between jobs

### Expected Test Output
```
Starting Redis System Tests
==================================================
✓ Redis Connection
✓ Queue Manager
✓ Shared Store
✓ Job Processing
✓ Multiple Jobs
✓ Job Isolation

==================================================
Test Results Summary
==================================================
Tests Run: 6
Passed: 6
Failed: 0
Success Rate: 100.0%
```

## Debugging

### Check Redis Data
```bash
# List all keys
docker-compose exec redis redis-cli keys "*"

# Check queue size
docker-compose exec redis redis-cli llen pf:queue

# Inspect specific job data
docker-compose exec redis redis-cli keys "pf:abc123:*"
docker-compose exec redis redis-cli get "pf:abc123:_status"
```

### Worker Logs
```bash
# Live worker logs
docker-compose logs -f worker1 worker2

# Search for errors
docker-compose logs worker1 | grep ERROR
docker-compose logs worker2 | grep ERROR
```

### System Health
```bash
# Check all services
docker-compose ps

# Service health
docker-compose exec redis redis-cli ping
docker-compose exec worker1 echo "Worker 1 OK"
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (including Redis data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Scaling Workers

Add more workers by modifying `docker-compose.yml`:

```yaml
  worker3:
    build: .
    container_name: pocketflow-worker3
    depends_on:
      redis:
        condition: service_healthy
    environment:
      - REDIS_URL=redis://redis:6379
      - WORKER_ID=3
    command: python cookbook/pocketflow-redis-example/worker.py --worker-id 3 --redis-url redis://redis:6379
    restart: unless-stopped
```

Or scale dynamically:
```bash
docker-compose up --scale worker1=5
```

## Production Considerations

- Use Redis clustering for high availability
- Implement proper logging and monitoring
- Set up health checks and restart policies
- Use environment-specific configurations
- Implement backup strategies for Redis data
- Monitor memory usage and set appropriate limits