#!/usr/bin/env python3
"""
Redis System Test Script
========================

Comprehensive test script to validate the Redis-based PocketFlow system.
This script tests job creation, processing, monitoring, and cleanup.
"""

import time
import logging
import sys
from typing import List, Dict, Any
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


class TestResults:
    """Track test results"""
    
    def __init__(self):
        self.tests_run = 0
        self.tests_passed = 0
        self.tests_failed = 0
        self.failures = []
    
    def record_test(self, test_name: str, passed: bool, error: str = None):
        self.tests_run += 1
        if passed:
            self.tests_passed += 1
            logger.info(f"✓ {test_name}")
        else:
            self.tests_failed += 1
            self.failures.append(f"{test_name}: {error}")
            logger.error(f"✗ {test_name}: {error}")
    
    def print_summary(self):
        print(f"\n{'='*50}")
        print(f"Test Results Summary")
        print(f"{'='*50}")
        print(f"Tests Run: {self.tests_run}")
        print(f"Passed: {self.tests_passed}")
        print(f"Failed: {self.tests_failed}")
        print(f"Success Rate: {self.tests_passed/self.tests_run*100:.1f}%")
        
        if self.failures:
            print(f"\nFailures:")
            for failure in self.failures:
                print(f"  - {failure}")


def test_redis_connection() -> bool:
    """Test Redis connection"""
    try:
        from redis import Redis
        r = Redis(host='redis', port=6379, decode_responses=True)
        r.ping()
        return True
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False


def test_queue_manager() -> bool:
    """Test basic queue manager operations"""
    try:
        qm = RedisQueueManager(redis_url="redis://redis:6379")
        
        # Test queue size
        initial_size = qm.queue_size()
        logger.info(f"Initial queue size: {initial_size}")
        
        # Test job creation
        job_id = qm.create_job("TestNode", {"test": "value"})
        logger.info(f"Created job: {job_id}")
        
        # Verify queue size increased
        new_size = qm.queue_size()
        if new_size != initial_size + 1:
            raise ValueError(f"Queue size should be {initial_size + 1}, got {new_size}")
        
        # Test dequeue
        message = qm.dequeue(timeout=1)
        if not message:
            raise ValueError("Failed to dequeue message")
        
        if message.job_id != job_id:
            raise ValueError(f"Job ID mismatch: expected {job_id}, got {message.job_id}")
        
        logger.info(f"Dequeued job: {message.job_id}")
        return True
        
    except Exception as e:
        logger.error(f"Queue manager test failed: {e}")
        return False


def test_shared_store() -> bool:
    """Test shared store with job isolation"""
    try:
        job_id1 = "test-job-1"
        job_id2 = "test-job-2"
        
        store1 = RedisSharedStore(job_id1, redis_url="redis://redis:6379")
        store2 = RedisSharedStore(job_id2, redis_url="redis://redis:6379")
        
        # Test basic operations
        store1["key1"] = "value1"
        store2["key1"] = "value2"
        
        # Test isolation
        if store1["key1"] == store2["key1"]:
            raise ValueError("Shared stores are not isolated")
        
        # Test get with default
        default_val = store1.get("nonexistent", "default")
        if default_val != "default":
            raise ValueError(f"Default value failed: expected 'default', got {default_val}")
        
        # Test contains
        if "key1" not in store1:
            raise ValueError("Contains check failed")
        
        # Test keys
        keys = store1.keys()
        if "key1" not in keys:
            raise ValueError("Keys listing failed")
        
        # Test status operations
        store1.set_status("running")
        status = store1.get_status()
        if status != "running":
            raise ValueError(f"Status operations failed: expected 'running', got {status}")
        
        # Test cleanup
        store1.clear()
        if store1.keys():
            raise ValueError("Clear operation failed")
        
        logger.info("Shared store isolation and operations verified")
        return True
        
    except Exception as e:
        logger.error(f"Shared store test failed: {e}")
        return False


def test_job_processing() -> bool:
    """Test end-to-end job processing"""
    try:
        qm = RedisQueueManager(redis_url="redis://redis:6379")
        
        # Create a simple job
        job_id = qm.create_job("LoadDataNode", {"filename": "test.txt"})
        logger.info(f"Created job for processing: {job_id}")
        
        # Wait for workers to process the job
        max_wait = 30  # 30 seconds max wait
        wait_interval = 2
        waited = 0
        
        shared = RedisSharedStore(job_id, redis_url="redis://redis:6379")
        
        while waited < max_wait:
            status = shared.get_status()
            logger.info(f"Job {job_id} status: {status}")
            
            if status == "completed":
                result = qm.get_job_result(job_id)
                logger.info(f"Job completed with result: {result}")
                
                # Verify shared store has expected data
                if "raw_data" not in shared:
                    raise ValueError("Expected 'raw_data' in shared store")
                
                if "processed_data" not in shared:
                    raise ValueError("Expected 'processed_data' in shared store")
                
                if "save_result" not in shared:
                    raise ValueError("Expected 'save_result' in shared store")
                
                return True
            
            elif status == "failed":
                error = shared.get("error", "Unknown error")
                raise ValueError(f"Job failed: {error}")
            
            time.sleep(wait_interval)
            waited += wait_interval
        
        raise ValueError(f"Job processing timed out after {max_wait} seconds")
        
    except Exception as e:
        logger.error(f"Job processing test failed: {e}")
        return False


def test_multiple_jobs() -> bool:
    """Test multiple concurrent jobs"""
    try:
        qm = RedisQueueManager(redis_url="redis://redis:6379")
        
        # Create multiple jobs
        job_ids = []
        for i in range(3):
            job_id = qm.create_job("LoadDataNode", {"filename": f"test_{i}.txt"})
            job_ids.append(job_id)
        
        logger.info(f"Created {len(job_ids)} jobs")
        
        # Wait for all jobs to complete
        max_wait = 60  # 60 seconds for multiple jobs
        wait_interval = 3
        waited = 0
        
        while waited < max_wait:
            completed = 0
            failed = 0
            
            for job_id in job_ids:
                shared = RedisSharedStore(job_id, redis_url="redis://redis:6379")
                status = shared.get_status()
                
                if status == "completed":
                    completed += 1
                elif status == "failed":
                    failed += 1
            
            logger.info(f"Job status: {completed} completed, {failed} failed, {len(job_ids) - completed - failed} in progress")
            
            if completed == len(job_ids):
                logger.info("All jobs completed successfully")
                return True
            
            if failed > 0:
                raise ValueError(f"{failed} jobs failed")
            
            time.sleep(wait_interval)
            waited += wait_interval
        
        raise ValueError(f"Multiple jobs test timed out after {max_wait} seconds")
        
    except Exception as e:
        logger.error(f"Multiple jobs test failed: {e}")
        return False


def test_job_isolation() -> bool:
    """Test that jobs are properly isolated"""
    try:
        qm = RedisQueueManager(redis_url="redis://redis:6379")
        
        # Create two jobs with different parameters
        job_id1 = qm.create_job("LoadDataNode", {"filename": "isolated_1.txt"})
        job_id2 = qm.create_job("LoadDataNode", {"filename": "isolated_2.txt"})
        
        # Wait for jobs to complete
        max_wait = 30
        wait_interval = 2
        waited = 0
        
        while waited < max_wait:
            shared1 = RedisSharedStore(job_id1, redis_url="redis://redis:6379")
            shared2 = RedisSharedStore(job_id2, redis_url="redis://redis:6379")
            
            status1 = shared1.get_status()
            status2 = shared2.get_status()
            
            if status1 == "completed" and status2 == "completed":
                # Check that data is isolated
                data1 = shared1.get("raw_data")
                data2 = shared2.get("raw_data")
                
                if data1 == data2:
                    raise ValueError("Job data is not isolated")
                
                if "isolated_1.txt" not in data1:
                    raise ValueError("Job 1 data doesn't contain expected filename")
                
                if "isolated_2.txt" not in data2:
                    raise ValueError("Job 2 data doesn't contain expected filename")
                
                logger.info("Job isolation verified")
                return True
            
            if status1 == "failed" or status2 == "failed":
                raise ValueError("One or more jobs failed")
            
            time.sleep(wait_interval)
            waited += wait_interval
        
        raise ValueError(f"Job isolation test timed out after {max_wait} seconds")
        
    except Exception as e:
        logger.error(f"Job isolation test failed: {e}")
        return False


def main():
    """Run all tests"""
    results = TestResults()
    
    print("Starting Redis System Tests")
    print("=" * 50)
    
    # Test Redis connection
    results.record_test("Redis Connection", test_redis_connection())
    
    # Test queue manager
    results.record_test("Queue Manager", test_queue_manager())
    
    # Test shared store
    results.record_test("Shared Store", test_shared_store())
    
    # Test job processing
    results.record_test("Job Processing", test_job_processing())
    
    # Test multiple jobs
    results.record_test("Multiple Jobs", test_multiple_jobs())
    
    # Test job isolation
    results.record_test("Job Isolation", test_job_isolation())
    
    # Print summary
    results.print_summary()
    
    # Exit with appropriate code
    sys.exit(0 if results.tests_failed == 0 else 1)


if __name__ == "__main__":
    main()