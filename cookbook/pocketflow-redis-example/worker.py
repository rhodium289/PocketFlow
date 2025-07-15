#!/usr/bin/env python3
"""
Distributed Redis Worker
========================

A production-ready worker that processes PocketFlow jobs from Redis queue.
This worker can be run across multiple processes or machines for scalability.

Usage:
    python worker.py --worker-id 1
    python worker.py --worker-id 2 --redis-url redis://localhost:6379
"""

import argparse
import logging
import sys
import time
from pocketflow import Node, AsyncNode, REDIS_AVAILABLE

if not REDIS_AVAILABLE:
    print("Redis not available. Please install: pip install redis")
    sys.exit(1)

from pocketflow import RedisWorker, RedisQueueManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LoadDataNode(Node):
    """Load data from a file or database"""
    
    def prep(self, shared):
        filename = self.params.get("filename", "default.txt")
        return filename
    
    def exec(self, filename):
        # Simulate file loading
        time.sleep(0.1)
        logger.info(f"Loading data from {filename}")
        return f"Data loaded from {filename}"
    
    def post(self, shared, prep_res, exec_res):
        shared["raw_data"] = exec_res
        shared["load_time"] = time.time()
        logger.info(f"Data loaded: {exec_res}")
        return "process"


class ProcessDataNode(Node):
    """Process the loaded data"""
    
    def prep(self, shared):
        raw_data = shared.get("raw_data", "")
        processing_type = self.params.get("type", "uppercase")
        return {"data": raw_data, "type": processing_type}
    
    def exec(self, prep_data):
        # Simulate data processing
        time.sleep(0.2)
        data = prep_data["data"]
        processing_type = prep_data["type"]
        
        if processing_type == "uppercase":
            result = data.upper()
        elif processing_type == "lowercase":
            result = data.lower()
        elif processing_type == "reverse":
            result = data[::-1]
        else:
            result = data
        
        logger.info(f"Processing {data} with type {processing_type}")
        return f"{result} [PROCESSED]"
    
    def post(self, shared, prep_res, exec_res):
        shared["processed_data"] = exec_res
        shared["process_time"] = time.time()
        logger.info(f"Processed: {exec_res}")
        return "save"


class SaveDataNode(Node):
    """Save the processed data"""
    
    def prep(self, shared):
        processed_data = shared.get("processed_data", "")
        output_file = self.params.get("output", "output.txt")
        return {"data": processed_data, "file": output_file}
    
    def exec(self, prep_data):
        # Simulate saving data
        time.sleep(0.1)
        logger.info(f"Saving to {prep_data['file']}: {prep_data['data']}")
        return f"Saved to {prep_data['file']}"
    
    def post(self, shared, prep_res, exec_res):
        shared["save_result"] = exec_res
        shared["save_time"] = time.time()
        
        # Calculate total processing time
        load_time = shared.get("load_time", 0)
        save_time = shared.get("save_time", 0)
        total_time = save_time - load_time
        
        shared["total_processing_time"] = total_time
        logger.info(f"Save complete: {exec_res} (total time: {total_time:.2f}s)")
        return "complete"


class ErrorNode(Node):
    """Node that simulates errors for testing"""
    
    def prep(self, shared):
        return self.params.get("error_type", "generic")
    
    def exec(self, error_type):
        if error_type == "timeout":
            time.sleep(10)  # Simulate timeout
        elif error_type == "exception":
            raise ValueError("Simulated error")
        else:
            raise RuntimeError(f"Unknown error type: {error_type}")
    
    def post(self, shared, prep_res, exec_res):
        return "complete"


class RetryNode(Node):
    """Node that demonstrates retry functionality"""
    
    def __init__(self, max_retries=3, wait=1):
        super().__init__(max_retries=max_retries, wait=wait)
        self.attempt_count = 0
    
    def prep(self, shared):
        return self.params.get("success_after", 2)
    
    def exec(self, success_after):
        self.attempt_count += 1
        logger.info(f"Retry attempt {self.attempt_count}")
        
        if self.attempt_count < success_after:
            raise RuntimeError(f"Retry {self.attempt_count} failed")
        
        return f"Success after {self.attempt_count} attempts"
    
    def post(self, shared, prep_res, exec_res):
        shared["retry_result"] = exec_res
        logger.info(f"Retry successful: {exec_res}")
        return "complete"


def create_node_registry():
    """Create the node registry for this worker"""
    
    # Create node instances
    load_node = LoadDataNode()
    process_node = ProcessDataNode()
    save_node = SaveDataNode()
    error_node = ErrorNode()
    retry_node = RetryNode()
    
    # Wire up the main flow
    load_node.next(process_node, "process")
    process_node.next(save_node, "save")
    
    # Create registry
    return {
        "LoadDataNode": load_node,
        "ProcessDataNode": process_node,
        "SaveDataNode": save_node,
        "ErrorNode": error_node,
        "RetryNode": retry_node
    }

def create_node_registry_sugar_syntax():
    """Create the node registry for this worker"""

    # Create node instances
    load_node = LoadDataNode()
    process_node = ProcessDataNode()
    save_node = SaveDataNode()
    error_node = ErrorNode()
    retry_node = RetryNode()

    # Wire up the main flow using sugar syntax
    load_node - "process" >> process_node
    process_node - "save" >> save_node

    # Create registry
    return {
        "LoadDataNode": load_node,
        "ProcessDataNode": process_node,
        "SaveDataNode": save_node,
        "ErrorNode": error_node,
        "RetryNode": retry_node
    }

def main():
    parser = argparse.ArgumentParser(description="Redis Worker for PocketFlow")
    parser.add_argument("--worker-id", type=str, required=True,
                       help="Unique identifier for this worker")
    parser.add_argument("--redis-url", type=str, default=None,
                       help="Redis connection URL (defaults to env vars)")
    parser.add_argument("--queue-name", type=str, default="pf:queue",
                       help="Redis queue name")
    parser.add_argument("--timeout", type=int, default=0,
                       help="Queue timeout in seconds (0 for blocking)")
    parser.add_argument("--stats-interval", type=int, default=30,
                       help="Stats reporting interval in seconds")
    
    args = parser.parse_args()
    
    # Test Redis connection
    try:
        from redis import Redis
        # Use the same Redis URL resolution as RedisQueueManager
        logger.info(f"Debug: args.redis_url = {repr(args.redis_url)}")
        
        queue_manager_temp = RedisQueueManager(redis_url=args.redis_url)
        redis_url = queue_manager_temp.redis_url
        
        logger.info(f"Debug: resolved redis_url = {repr(redis_url)}")
        
        if not redis_url:
            raise ValueError("Redis URL is None or empty")
        
        r = Redis.from_url(redis_url, decode_responses=True)
        r.ping()
        logger.info(f"✓ Connected to Redis at {redis_url}")
    except Exception as e:
        logger.error(f"✗ Redis connection failed: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    
    # Create node registry
    node_registry = create_node_registry_sugar_syntax()
    logger.info(f"Registered nodes: {list(node_registry.keys())}")
    
    # Create queue manager and worker
    queue_manager = RedisQueueManager(
        redis_url=args.redis_url,  # Will use env vars if None
        queue_name=args.queue_name
    )
    
    worker = RedisWorker(
        node_registry=node_registry,
        queue_manager=queue_manager,
        worker_name=f"worker-{args.worker_id}"
    )
    
    # Stats reporting
    last_stats_time = time.time()
    last_processed_count = 0
    
    logger.info(f"Starting worker {args.worker_id}")
    
    try:
        while True:
            try:
                # Process messages
                message = queue_manager.dequeue(timeout=args.timeout or 1)
                if message:
                    worker.process_message(message)
            except Exception as msg_error:
                logger.error(f"Error processing message: {msg_error}")
                import traceback
                logger.error(f"Message processing traceback: {traceback.format_exc()}")
                # Continue running instead of crashing
            
            # Report stats periodically
            current_time = time.time()
            if current_time - last_stats_time >= args.stats_interval:
                current_count = worker.processed_count
                rate = (current_count - last_processed_count) / args.stats_interval
                queue_size = queue_manager.queue_size()
                
                logger.info(f"Worker {args.worker_id} stats: "
                           f"processed={current_count}, "
                           f"rate={rate:.2f}/s, "
                           f"queue_size={queue_size}")
                
                last_stats_time = current_time
                last_processed_count = current_count
    
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    except Exception as e:
        logger.error(f"Worker error: {e}")
        sys.exit(1)
    
    logger.info(f"Worker {args.worker_id} stopped. Total processed: {worker.processed_count}")


if __name__ == "__main__":
    main()