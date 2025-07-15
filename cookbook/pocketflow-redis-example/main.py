#!/usr/bin/env python3
"""
PocketFlow Redis Example
========================

This example demonstrates how to use PocketFlow with Redis for distributed
job execution and shared data storage with job_id isolation.

Features demonstrated:
- Redis-backed shared store with job_id isolation
- Queue-based job execution
- Distributed worker processing
- Job status tracking

Requirements:
- Redis server running on localhost:6379
- pip install redis
"""

import asyncio
import logging
import time
from pocketflow import Node, AsyncNode, REDIS_AVAILABLE

if not REDIS_AVAILABLE:
    print("Redis not available. Please install: pip install redis")
    exit(1)

from pocketflow import (
    RedisSharedStore, AsyncRedisSharedStore,
    RedisQueueManager, AsyncRedisQueueManager,
    RedisWorker, AsyncRedisWorker,
    QueueMessage
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LoadDataNode(Node):
    """Load initial data and store in shared memory"""
    
    def prep(self, shared):
        return self.params.get("filename", "data.txt")
    
    def exec(self, filename):
        # Simulate loading data
        time.sleep(0.1)
        return f"Data loaded from {filename}"
    
    def post(self, shared, prep_res, exec_res):
        shared["raw_data"] = exec_res
        shared["processing_start"] = time.time()
        logger.info(f"Loaded data: {exec_res}")
        return "process"


class ProcessDataNode(Node):
    """Process the loaded data"""
    
    def prep(self, shared):
        return shared.get("raw_data", "No data")
    
    def exec(self, raw_data):
        # Simulate processing
        time.sleep(0.2)
        processed = raw_data.upper() + " [PROCESSED]"
        return processed
    
    def post(self, shared, prep_res, exec_res):
        shared["processed_data"] = exec_res
        logger.info(f"Processed data: {exec_res}")
        return "save"


class SaveDataNode(Node):
    """Save the processed data"""
    
    def prep(self, shared):
        return {
            "data": shared.get("processed_data", "No data"),
            "start_time": shared.get("processing_start", 0)
        }
    
    def exec(self, prep_data):
        # Simulate saving
        time.sleep(0.1)
        processing_time = time.time() - prep_data["start_time"]
        return f"Saved: {prep_data['data']} (took {processing_time:.2f}s)"
    
    def post(self, shared, prep_res, exec_res):
        shared["result"] = exec_res
        shared["processing_end"] = time.time()
        logger.info(f"Final result: {exec_res}")
        return "complete"


class AsyncLoadDataNode(AsyncNode):
    """Async version of LoadDataNode"""
    
    async def prep_async(self, shared):
        return self.params.get("filename", "data.txt")
    
    async def exec_async(self, filename):
        # Simulate async loading
        await asyncio.sleep(0.1)
        return f"Async data loaded from {filename}"
    
    async def post_async(self, shared, prep_res, exec_res):
        await shared.__setitem__("raw_data", exec_res)
        await shared.__setitem__("processing_start", time.time())
        logger.info(f"Async loaded data: {exec_res}")
        return "process"


class AsyncProcessDataNode(AsyncNode):
    """Async version of ProcessDataNode"""
    
    async def prep_async(self, shared):
        return await shared.get("raw_data", "No data")
    
    async def exec_async(self, raw_data):
        # Simulate async processing
        await asyncio.sleep(0.2)
        processed = raw_data.upper() + " [ASYNC PROCESSED]"
        return processed
    
    async def post_async(self, shared, prep_res, exec_res):
        await shared.__setitem__("processed_data", exec_res)
        logger.info(f"Async processed data: {exec_res}")
        return "save"


class AsyncSaveDataNode(AsyncNode):
    """Async version of SaveDataNode"""
    
    async def prep_async(self, shared):
        return {
            "data": await shared.get("processed_data", "No data"),
            "start_time": await shared.get("processing_start", 0)
        }
    
    async def exec_async(self, prep_data):
        # Simulate async saving
        await asyncio.sleep(0.1)
        processing_time = time.time() - prep_data["start_time"]
        return f"Async saved: {prep_data['data']} (took {processing_time:.2f}s)"
    
    async def post_async(self, shared, prep_res, exec_res):
        await shared.__setitem__("result", exec_res)
        await shared.__setitem__("processing_end", time.time())
        logger.info(f"Async final result: {exec_res}")
        return "complete"


def demo_sync_redis_flow():
    """Demonstrate synchronous Redis-based flow"""
    print("\n=== Synchronous Redis Flow Demo ===")
    
    # Create nodes
    load_node = LoadDataNode()
    process_node = ProcessDataNode()
    save_node = SaveDataNode()
    
    # Wire up the flow
    load_node.next(process_node, "process")
    process_node.next(save_node, "save")
    
    # Create node registry for workers
    node_registry = {
        "LoadDataNode": load_node,
        "ProcessDataNode": process_node,
        "SaveDataNode": save_node
    }
    
    # Create queue manager and worker
    queue_manager = RedisQueueManager()
    worker = RedisWorker(node_registry, queue_manager)
    
    # Create and start a job
    job_id = queue_manager.create_job("LoadDataNode", {"filename": "example.txt"})
    print(f"Created job: {job_id}")
    
    # Process jobs (in a real scenario, this would run in a separate process)
    print("Processing jobs...")
    processed = 0
    max_jobs = 3  # Load, Process, Save
    
    while processed < max_jobs:
        message = queue_manager.dequeue(timeout=1)
        if message:
            worker.process_message(message)
            processed += 1
        else:
            break
    
    # Check final result
    result = queue_manager.get_job_result(job_id)
    print(f"Job result: {result}")
    
    # Check shared store
    shared = RedisSharedStore(job_id)
    print(f"Job status: {shared.get_status()}")
    print(f"Final result in shared store: {shared.get('result')}")


async def demo_async_redis_flow():
    """Demonstrate asynchronous Redis-based flow"""
    print("\n=== Asynchronous Redis Flow Demo ===")
    
    # Create async nodes
    load_node = AsyncLoadDataNode()
    process_node = AsyncProcessDataNode()
    save_node = AsyncSaveDataNode()
    
    # Wire up the flow
    load_node.next(process_node, "process")
    process_node.next(save_node, "save")
    
    # Create node registry for workers
    node_registry = {
        "AsyncLoadDataNode": load_node,
        "AsyncProcessDataNode": process_node,
        "AsyncSaveDataNode": save_node
    }
    
    # Create queue manager and worker
    queue_manager = AsyncRedisQueueManager()
    worker = AsyncRedisWorker(node_registry, queue_manager)
    
    # Create and start a job
    job_id = await queue_manager.create_job("AsyncLoadDataNode", {"filename": "async_example.txt"})
    print(f"Created async job: {job_id}")
    
    # Process jobs (in a real scenario, this would run in a separate process)
    print("Processing async jobs...")
    processed = 0
    max_jobs = 3  # Load, Process, Save
    
    while processed < max_jobs:
        message = await queue_manager.dequeue(timeout=1)
        if message:
            await worker.process_message(message)
            processed += 1
        else:
            break
    
    # Check final result
    result = await queue_manager.get_job_result(job_id)
    print(f"Async job result: {result}")
    
    # Check shared store
    shared = AsyncRedisSharedStore(job_id)
    print(f"Async job status: {await shared.get_status()}")
    print(f"Final result in shared store: {await shared.get('result')}")


def demo_job_isolation():
    """Demonstrate job isolation with multiple concurrent jobs"""
    print("\n=== Job Isolation Demo ===")
    
    # Create nodes
    load_node = LoadDataNode()
    process_node = ProcessDataNode()
    save_node = SaveDataNode()
    
    # Wire up the flow
    load_node.next(process_node, "process")
    process_node.next(save_node, "save")
    
    # Create node registry for workers
    node_registry = {
        "LoadDataNode": load_node,
        "ProcessDataNode": process_node,
        "SaveDataNode": save_node
    }
    
    # Create queue manager and worker
    queue_manager = RedisQueueManager()
    worker = RedisWorker(node_registry, queue_manager)
    
    # Create multiple jobs
    job_ids = []
    for i in range(3):
        job_id = queue_manager.create_job("LoadDataNode", {"filename": f"file_{i}.txt"})
        job_ids.append(job_id)
        print(f"Created job {i}: {job_id}")
    
    # Process all jobs
    print("Processing all jobs...")
    total_messages = 9  # 3 jobs * 3 nodes each
    processed = 0
    
    while processed < total_messages:
        message = queue_manager.dequeue(timeout=1)
        if message:
            worker.process_message(message)
            processed += 1
        else:
            break
    
    # Check results for all jobs
    print("\nJob results:")
    for i, job_id in enumerate(job_ids):
        result = queue_manager.get_job_result(job_id)
        shared = RedisSharedStore(job_id)
        print(f"Job {i} ({job_id}): {shared.get_status()}")
        print(f"  Result: {result}")
        print(f"  Data keys: {shared.keys()}")


if __name__ == "__main__":
    print("PocketFlow Redis Example")
    print("=" * 50)
    
    try:
        # Test Redis connection
        from redis import Redis
        r = Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("Please make sure Redis is running on localhost:6379")
        exit(1)
    
    # Run demos
    demo_sync_redis_flow()
    
    # Run async demo
    asyncio.run(demo_async_redis_flow())
    
    # Demo job isolation
    demo_job_isolation()
    
    print("\n=== Demo Complete ===")
    print("Key features demonstrated:")
    print("- Redis-backed shared store with job_id isolation")
    print("- Queue-based distributed job execution")
    print("- Both sync and async node processing")
    print("- Multiple concurrent jobs with isolated data")