#!/usr/bin/env python3
"""
Job Creator for Redis Workers
=============================

Creates and submits jobs to the Redis queue for processing by distributed workers.
Demonstrates various job types and monitoring capabilities.

Usage:
    python job_creator.py --num-jobs 10
    python job_creator.py --job-type batch --batch-size 5
"""

import argparse
import logging
import sys
import time
from typing import List
from pocketflow import REDIS_AVAILABLE

if not REDIS_AVAILABLE:
    print("Redis not available. Please install: pip install redis")
    sys.exit(1)

from pocketflow import RedisQueueManager, RedisSharedStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_simple_job(queue_manager: RedisQueueManager, job_id: int) -> str:
    """Create a simple load->process->save job"""
    
    job_params = {
        "filename": f"input_{job_id}.txt",
        "type": "uppercase",
        "output": f"output_{job_id}.txt"
    }
    
    job_id = queue_manager.create_job("LoadDataNode", job_params)
    logger.info(f"Created simple job: {job_id}")
    return job_id


def create_batch_jobs(queue_manager: RedisQueueManager, batch_size: int) -> List[str]:
    """Create a batch of jobs with different processing types"""
    
    processing_types = ["uppercase", "lowercase", "reverse"]
    job_ids = []
    
    for i in range(batch_size):
        job_params = {
            "filename": f"batch_{i}.txt",
            "type": processing_types[i % len(processing_types)],
            "output": f"batch_output_{i}.txt"
        }
        
        job_id = queue_manager.create_job("LoadDataNode", job_params)
        job_ids.append(job_id)
        logger.info(f"Created batch job {i}: {job_id}")
    
    return job_ids


def create_retry_job(queue_manager: RedisQueueManager) -> str:
    """Create a job that demonstrates retry functionality"""
    
    job_params = {
        "success_after": 3  # Fail twice, succeed on third attempt
    }
    
    job_id = queue_manager.create_job("RetryNode", job_params)
    logger.info(f"Created retry job: {job_id}")
    return job_id


def create_error_job(queue_manager: RedisQueueManager, error_type: str) -> str:
    """Create a job that will fail with a specific error"""
    
    job_params = {
        "error_type": error_type
    }
    
    job_id = queue_manager.create_job("ErrorNode", job_params)
    logger.info(f"Created error job ({error_type}): {job_id}")
    return job_id


def monitor_job(queue_manager: RedisQueueManager, job_id: str, timeout: int = 60) -> dict:
    """Monitor a job until completion or timeout"""
    
    start_time = time.time()
    shared = RedisSharedStore(job_id)
    
    logger.info(f"Monitoring job {job_id}...")
    
    while time.time() - start_time < timeout:
        status = shared.get_status()
        
        if status == "completed":
            result = queue_manager.get_job_result(job_id)
            logger.info(f"Job {job_id} completed with result: {result}")
            
            return {
                "job_id": job_id,
                "status": status,
                "result": result,
                "data": {key: shared.get(key) for key in shared.keys()},
                "duration": time.time() - start_time
            }
        
        elif status == "failed":
            error = shared.get("error", "Unknown error")
            logger.error(f"Job {job_id} failed: {error}")
            
            return {
                "job_id": job_id,
                "status": status,
                "error": error,
                "data": {key: shared.get(key) for key in shared.keys()},
                "duration": time.time() - start_time
            }
        
        elif status:
            logger.info(f"Job {job_id} status: {status}")
        
        time.sleep(1)
    
    logger.warning(f"Job {job_id} monitoring timed out")
    return {
        "job_id": job_id,
        "status": "timeout",
        "duration": timeout
    }


def monitor_queue(queue_manager: RedisQueueManager, duration: int = 60):
    """Monitor queue statistics"""
    
    start_time = time.time()
    
    logger.info(f"Monitoring queue for {duration} seconds...")
    
    while time.time() - start_time < duration:
        queue_size = queue_manager.queue_size()
        logger.info(f"Queue size: {queue_size}")
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Job Creator for Redis Workers")
    parser.add_argument("--redis-url", type=str, default=None,
                       help="Redis connection URL (defaults to env vars)")
    parser.add_argument("--queue-name", type=str, default="pf:queue",
                       help="Redis queue name")
    parser.add_argument("--job-type", type=str, 
                       choices=["simple", "batch", "retry", "error", "monitor"],
                       default="simple",
                       help="Type of job to create")
    parser.add_argument("--num-jobs", type=int, default=1,
                       help="Number of jobs to create")
    parser.add_argument("--batch-size", type=int, default=5,
                       help="Batch size for batch jobs")
    parser.add_argument("--error-type", type=str, 
                       choices=["timeout", "exception", "generic"],
                       default="exception",
                       help="Type of error for error jobs")
    parser.add_argument("--monitor", action="store_true",
                       help="Monitor job progress")
    parser.add_argument("--monitor-timeout", type=int, default=60,
                       help="Job monitoring timeout in seconds")
    
    args = parser.parse_args()
    
    # Test Redis connection
    try:
        from redis import Redis
        # Use the same Redis URL resolution as RedisQueueManager
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
    
    # Create queue manager
    queue_manager = RedisQueueManager(
        redis_url=args.redis_url,
        queue_name=args.queue_name
    )
    
    job_ids = []
    
    # Create jobs based on type
    if args.job_type == "simple":
        for i in range(args.num_jobs):
            job_id = create_simple_job(queue_manager, i)
            job_ids.append(job_id)
    
    elif args.job_type == "batch":
        job_ids = create_batch_jobs(queue_manager, args.batch_size)
    
    elif args.job_type == "retry":
        for i in range(args.num_jobs):
            job_id = create_retry_job(queue_manager)
            job_ids.append(job_id)
    
    elif args.job_type == "error":
        for i in range(args.num_jobs):
            job_id = create_error_job(queue_manager, args.error_type)
            job_ids.append(job_id)
    
    elif args.job_type == "monitor":
        monitor_queue(queue_manager, args.monitor_timeout)
        return
    
    logger.info(f"Created {len(job_ids)} jobs")
    
    # Monitor jobs if requested
    if args.monitor:
        results = []
        for job_id in job_ids:
            result = monitor_job(queue_manager, job_id, args.monitor_timeout)
            results.append(result)
        
        # Print summary
        logger.info("\n=== Job Summary ===")
        completed = len([r for r in results if r["status"] == "completed"])
        failed = len([r for r in results if r["status"] == "failed"])
        timeouts = len([r for r in results if r["status"] == "timeout"])
        
        logger.info(f"Completed: {completed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Timeouts: {timeouts}")
        
        if results:
            avg_duration = sum(r["duration"] for r in results) / len(results)
            logger.info(f"Average duration: {avg_duration:.2f}s")
    
    else:
        logger.info("Jobs created. Use --monitor to track progress")
        logger.info(f"Queue size: {queue_manager.queue_size()}")


if __name__ == "__main__":
    main()