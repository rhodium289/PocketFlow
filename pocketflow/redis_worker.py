import asyncio
import logging
import signal
import time
from typing import Dict, Any, Optional, Type, Callable
from concurrent.futures import ThreadPoolExecutor

from . import BaseNode, Node, AsyncNode
from .redis_backend import (
    RedisQueueManager, AsyncRedisQueueManager, 
    RedisSharedStore, AsyncRedisSharedStore,
    QueueMessage
)

logger = logging.getLogger(__name__)


class RedisWorker:
    """Synchronous Redis worker that processes jobs from the queue"""
    
    def __init__(self, node_registry: Dict[str, BaseNode], 
                 queue_manager: RedisQueueManager = None,
                 worker_name: str = None):
        # Defensive programming: validate arguments
        if not node_registry or not isinstance(node_registry, dict):
            raise ValueError("node_registry must be a non-empty dictionary")
        if not all(isinstance(k, str) and hasattr(v, 'run') for k, v in node_registry.items()):
            raise ValueError("node_registry must contain string keys and node objects with run() method")
        if worker_name and not isinstance(worker_name, str):
            raise ValueError("worker_name must be a string")
        
        self.node_registry = node_registry
        self.queue_manager = queue_manager or RedisQueueManager()
        self.worker_name = worker_name or f"worker-{int(time.time())}"
        self.running = False
        self.processed_count = 0
        
    def register_node(self, name: str, node: BaseNode) -> None:
        """Register a node with the worker"""
        self.node_registry[name] = node
        
    def process_message(self, message: QueueMessage) -> None:
        """Process a single message from the queue"""
        # Defensive programming: validate message
        if not message or not isinstance(message, QueueMessage):
            logger.error("Invalid message: must be a QueueMessage instance")
            return
        if not message.job_id or not isinstance(message.job_id, str):
            logger.error("Invalid message: job_id must be a non-empty string")
            return
        if not message.node_name or not isinstance(message.node_name, str):
            logger.error("Invalid message: node_name must be a non-empty string")
            return
        
        logger.info(f"Processing job {message.job_id}, node {message.node_name}")
        
        # Get the node instance
        if message.node_name not in self.node_registry:
            logger.error(f"Node {message.node_name} not found in registry")
            return
            
        node = self.node_registry[message.node_name]
        
        # Set up job-specific shared store with worker's Redis URL
        shared = RedisSharedStore(message.job_id, redis_url=self.queue_manager.redis_url)
        shared.set_status("processing")
        
        try:
            # Set node parameters
            if message.params:
                node.set_params(message.params)
            
            # Debug: Check shared store Redis URL
            logger.info(f"Worker Redis URL: {self.queue_manager.redis_url}")
            logger.info(f"Shared store Redis URL: {shared.redis_url}")
            
            # Execute the node
            action = node.run(shared)
            
            # Check if there's a successor node
            if action and action in node.successors:
                next_node = node.successors[action]
                next_message = QueueMessage(
                    job_id=message.job_id,
                    node_name=next_node.__class__.__name__,
                    action=action,
                    params=next_node.params
                )
                self.queue_manager.enqueue(next_message)
                logger.info(f"Enqueued next node: {next_node.__class__.__name__}")
            elif action:
                # Job completed - no successor found
                shared.set_status("completed")
                self.queue_manager.set_job_result(message.job_id, action)
                logger.info(f"Job {message.job_id} completed with result: {action}")
            else:
                # Node returned None - job may be complete
                shared.set_status("completed")
                self.queue_manager.set_job_result(message.job_id, None)
                logger.info(f"Job {message.job_id} completed")
                
            self.processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing job {message.job_id}: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            try:
                shared.set_status("failed")
                shared["error"] = str(e)
                shared["worker"] = self.worker_name
            except Exception as cleanup_e:
                logger.error(f"Failed to set error status: {cleanup_e}")
    
    def run(self, timeout: int = 0) -> None:
        """Run the worker loop"""
        self.running = True
        logger.info(f"Starting worker {self.worker_name}")
        
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.running = False
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        while self.running:
            try:
                message = self.queue_manager.dequeue(timeout=timeout)
                if message:
                    self.process_message(message)
                elif timeout == 0:
                    # Non-blocking mode - sleep briefly to avoid busy waiting
                    time.sleep(0.1)
                    
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}")
                time.sleep(1)  # Brief pause before continuing
                
        logger.info(f"Worker {self.worker_name} stopped. Processed {self.processed_count} jobs.")


class AsyncRedisWorker:
    """Asynchronous Redis worker that processes jobs from the queue"""
    
    def __init__(self, node_registry: Dict[str, BaseNode], 
                 queue_manager: AsyncRedisQueueManager = None,
                 worker_name: str = None,
                 max_concurrent_jobs: int = 10):
        self.node_registry = node_registry
        self.queue_manager = queue_manager or AsyncRedisQueueManager()
        self.worker_name = worker_name or f"async-worker-{int(time.time())}"
        self.max_concurrent_jobs = max_concurrent_jobs
        self.running = False
        self.processed_count = 0
        self.semaphore = asyncio.Semaphore(max_concurrent_jobs)
        
    def register_node(self, name: str, node: BaseNode) -> None:
        """Register a node with the worker"""
        self.node_registry[name] = node
        
    async def process_message(self, message: QueueMessage) -> None:
        """Process a single message from the queue"""
        async with self.semaphore:
            logger.info(f"Processing job {message.job_id}, node {message.node_name}")
            
            # Get the node instance
            if message.node_name not in self.node_registry:
                logger.error(f"Node {message.node_name} not found in registry")
                return
                
            node = self.node_registry[message.node_name]
            
            # Set up job-specific shared store with worker's Redis URL
            shared = AsyncRedisSharedStore(message.job_id, redis_url=self.queue_manager.redis_url)
            await shared.set_status("processing")
            
            try:
                # Set node parameters
                if message.params:
                    node.set_params(message.params)
                
                # Execute the node (async or sync)
                if isinstance(node, AsyncNode):
                    action = await node.run_async(shared)
                else:
                    action = node.run(shared)
                
                # Check if there's a successor node
                if action and action in node.successors:
                    next_node = node.successors[action]
                    next_message = QueueMessage(
                        job_id=message.job_id,
                        node_name=next_node.__class__.__name__,
                        action=action,
                        params=next_node.params
                    )
                    await self.queue_manager.enqueue(next_message)
                    logger.info(f"Enqueued next node: {next_node.__class__.__name__}")
                elif action:
                    # Job completed - no successor found
                    await shared.set_status("completed")
                    await self.queue_manager.set_job_result(message.job_id, action)
                    logger.info(f"Job {message.job_id} completed with result: {action}")
                else:
                    # Node returned None - job may be complete
                    await shared.set_status("completed")
                    await self.queue_manager.set_job_result(message.job_id, None)
                    logger.info(f"Job {message.job_id} completed")
                    
                self.processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing job {message.job_id}: {str(e)}")
                await shared.set_status("failed")
                await shared.__setitem__("error", str(e))
                await shared.__setitem__("worker", self.worker_name)
    
    async def run(self, timeout: int = 0) -> None:
        """Run the async worker loop"""
        self.running = True
        logger.info(f"Starting async worker {self.worker_name}")
        
        tasks = []
        
        try:
            while self.running:
                try:
                    message = await self.queue_manager.dequeue(timeout=timeout)
                    if message:
                        # Create a task for processing this message
                        task = asyncio.create_task(self.process_message(message))
                        tasks.append(task)
                        
                        # Clean up completed tasks
                        tasks = [t for t in tasks if not t.done()]
                        
                    elif timeout == 0:
                        # Non-blocking mode - sleep briefly to avoid busy waiting
                        await asyncio.sleep(0.1)
                        
                except asyncio.CancelledError:
                    logger.info("Worker cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in async worker loop: {str(e)}")
                    await asyncio.sleep(1)  # Brief pause before continuing
                    
        finally:
            # Wait for all running tasks to complete
            if tasks:
                logger.info(f"Waiting for {len(tasks)} running tasks to complete...")
                await asyncio.gather(*tasks, return_exceptions=True)
                
            logger.info(f"Async worker {self.worker_name} stopped. Processed {self.processed_count} jobs.")


class WorkerManager:
    """Manages multiple workers for load balancing"""
    
    def __init__(self, node_registry: Dict[str, BaseNode], 
                 queue_manager: RedisQueueManager = None,
                 num_workers: int = 4):
        self.node_registry = node_registry
        self.queue_manager = queue_manager or RedisQueueManager()
        self.num_workers = num_workers
        self.workers = []
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        
    def start(self) -> None:
        """Start all workers"""
        for i in range(self.num_workers):
            worker = RedisWorker(
                node_registry=self.node_registry.copy(),
                queue_manager=self.queue_manager,
                worker_name=f"worker-{i}"
            )
            self.workers.append(worker)
            
            # Submit worker to thread pool
            self.executor.submit(worker.run)
            
        logger.info(f"Started {self.num_workers} workers")
    
    def stop(self) -> None:
        """Stop all workers"""
        for worker in self.workers:
            worker.running = False
            
        self.executor.shutdown(wait=True)
        logger.info("All workers stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        return {
            "num_workers": len(self.workers),
            "total_processed": sum(w.processed_count for w in self.workers),
            "queue_size": self.queue_manager.queue_size(),
            "workers": [
                {
                    "name": w.worker_name,
                    "processed": w.processed_count,
                    "running": w.running
                }
                for w in self.workers
            ]
        }


class AsyncWorkerManager:
    """Manages multiple async workers for load balancing"""
    
    def __init__(self, node_registry: Dict[str, BaseNode], 
                 queue_manager: AsyncRedisQueueManager = None,
                 num_workers: int = 4):
        self.node_registry = node_registry
        self.queue_manager = queue_manager or AsyncRedisQueueManager()
        self.num_workers = num_workers
        self.workers = []
        self.tasks = []
        
    async def start(self) -> None:
        """Start all workers"""
        for i in range(self.num_workers):
            worker = AsyncRedisWorker(
                node_registry=self.node_registry.copy(),
                queue_manager=self.queue_manager,
                worker_name=f"async-worker-{i}"
            )
            self.workers.append(worker)
            
            # Create task for worker
            task = asyncio.create_task(worker.run())
            self.tasks.append(task)
            
        logger.info(f"Started {self.num_workers} async workers")
    
    async def stop(self) -> None:
        """Stop all workers"""
        for worker in self.workers:
            worker.running = False
            
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("All async workers stopped")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        return {
            "num_workers": len(self.workers),
            "total_processed": sum(w.processed_count for w in self.workers),
            "queue_size": await self.queue_manager.queue_size(),
            "workers": [
                {
                    "name": w.worker_name,
                    "processed": w.processed_count,
                    "running": w.running
                }
                for w in self.workers
            ]
        }