import asyncio
import json
import uuid
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

try:
    import redis
    import redis.asyncio as aioredis
except ImportError:
    raise ImportError("Redis not installed. Install with: pip install redis")

from . import BaseNode, Node, AsyncNode, Flow, AsyncFlow


@dataclass
class QueueMessage:
    """Message structure for Redis queue"""
    job_id: str
    node_name: str
    action: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "node_name": self.node_name,
            "action": self.action,
            "params": self.params or {},
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueueMessage":
        return cls(**data)


class RedisSharedStore:
    """Redis-backed shared store with job_id isolation"""
    
    def __init__(self, job_id: str, redis_url: str = None, 
                 prefix: str = "pf:", ttl: int = 3600):
        # Defensive programming: validate arguments
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")
        if prefix and not isinstance(prefix, str):
            raise ValueError("prefix must be a string")
        if ttl and not isinstance(ttl, int) or ttl < 0:
            raise ValueError("ttl must be a non-negative integer")
        
        self.job_id = job_id
        self.redis_url = redis_url or self._get_redis_url()
        
        # Defensive programming: ensure redis_url is valid
        if not self.redis_url:
            raise ValueError("Redis URL could not be determined from arguments or environment")
        
        self.prefix = f"{prefix}{job_id}:"
        self.ttl = ttl
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
    
    def _get_redis_url(self) -> str:
        """Get Redis URL from environment or default"""
        import os
        
        # Priority order for Redis URL detection
        redis_url = os.getenv('REDIS_URL')
        if redis_url and redis_url.strip():
            return redis_url.strip()
            
        redis_host = os.getenv('REDIS_HOST')
        if redis_host and redis_host.strip():
            redis_port = os.getenv('REDIS_PORT', '6379')
            if not redis_port or not redis_port.strip():
                redis_port = '6379'
            return f"redis://{redis_host.strip()}:{redis_port.strip()}"
            
        elasticache_endpoint = os.getenv('ELASTICACHE_ENDPOINT')
        if elasticache_endpoint and elasticache_endpoint.strip():
            return f"redis://{elasticache_endpoint.strip()}:6379"
            
        # Local development fallback
        return "redis://localhost:6379"
        
    def _key(self, key: str) -> str:
        """Generate prefixed key for job isolation"""
        return f"{self.prefix}{key}"
    
    def __getitem__(self, key: str) -> Any:
        val = self.r.get(self._key(key))
        return json.loads(val) if val else None
    
    def __setitem__(self, key: str, value: Any) -> None:
        json_val = json.dumps(value)
        self.r.set(self._key(key), json_val, ex=self.ttl)
    
    def get(self, key: str, default: Any = None) -> Any:
        result = self.__getitem__(key)
        return result if result is not None else default
    
    def __contains__(self, key: str) -> bool:
        return bool(self.r.exists(self._key(key)))
    
    def keys(self) -> List[str]:
        """Get all keys for this job"""
        pattern = f"{self.prefix}*"
        keys = self.r.keys(pattern)
        return [key.replace(self.prefix, "") for key in keys]
    
    def delete(self, key: str) -> bool:
        """Delete a key from the shared store"""
        return bool(self.r.delete(self._key(key)))
    
    def clear(self) -> None:
        """Clear all data for this job"""
        pattern = f"{self.prefix}*"
        keys = self.r.keys(pattern)
        if keys:
            self.r.delete(*keys)
    
    def set_status(self, status: str) -> None:
        """Set job status"""
        self["_status"] = status
    
    def get_status(self) -> Optional[str]:
        """Get job status"""
        return self.get("_status")


class AsyncRedisSharedStore:
    """Async Redis-backed shared store with job_id isolation"""
    
    def __init__(self, job_id: str, redis_url: str = None, 
                 prefix: str = "pf:", ttl: int = 3600):
        # Defensive programming: validate arguments
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")
        if prefix and not isinstance(prefix, str):
            raise ValueError("prefix must be a string")
        if ttl and not isinstance(ttl, int) or ttl < 0:
            raise ValueError("ttl must be a non-negative integer")
        
        self.job_id = job_id
        self.redis_url = redis_url or self._get_redis_url()
        
        # Defensive programming: ensure redis_url is valid
        if not self.redis_url:
            raise ValueError("Redis URL could not be determined from arguments or environment")
        
        self.prefix = f"{prefix}{job_id}:"
        self.ttl = ttl
        self.r = None
    
    def _get_redis_url(self) -> str:
        """Get Redis URL from environment or default"""
        import os
        
        # Priority order for Redis URL detection
        redis_url = os.getenv('REDIS_URL')
        if redis_url and redis_url.strip():
            return redis_url.strip()
            
        redis_host = os.getenv('REDIS_HOST')
        if redis_host and redis_host.strip():
            redis_port = os.getenv('REDIS_PORT', '6379')
            if not redis_port or not redis_port.strip():
                redis_port = '6379'
            return f"redis://{redis_host.strip()}:{redis_port.strip()}"
            
        elasticache_endpoint = os.getenv('ELASTICACHE_ENDPOINT')
        if elasticache_endpoint and elasticache_endpoint.strip():
            return f"redis://{elasticache_endpoint.strip()}:6379"
            
        # Local development fallback
        return "redis://localhost:6379"
        
    async def _get_redis(self):
        """Lazy initialization of Redis connection"""
        if self.r is None:
            self.r = aioredis.Redis.from_url(self.redis_url, decode_responses=True)
        return self.r
    
    def _key(self, key: str) -> str:
        """Generate prefixed key for job isolation"""
        return f"{self.prefix}{key}"
    
    async def __getitem__(self, key: str) -> Any:
        r = await self._get_redis()
        val = await r.get(self._key(key))
        return json.loads(val) if val else None
    
    async def __setitem__(self, key: str, value: Any) -> None:
        r = await self._get_redis()
        json_val = json.dumps(value)
        await r.set(self._key(key), json_val, ex=self.ttl)
    
    async def get(self, key: str, default: Any = None) -> Any:
        result = await self.__getitem__(key)
        return result if result is not None else default
    
    async def __contains__(self, key: str) -> bool:
        r = await self._get_redis()
        return bool(await r.exists(self._key(key)))
    
    async def keys(self) -> List[str]:
        """Get all keys for this job"""
        r = await self._get_redis()
        pattern = f"{self.prefix}*"
        keys = await r.keys(pattern)
        return [key.replace(self.prefix, "") for key in keys]
    
    async def delete(self, key: str) -> bool:
        """Delete a key from the shared store"""
        r = await self._get_redis()
        return bool(await r.delete(self._key(key)))
    
    async def clear(self) -> None:
        """Clear all data for this job"""
        r = await self._get_redis()
        pattern = f"{self.prefix}*"
        keys = await r.keys(pattern)
        if keys:
            await r.delete(*keys)
    
    async def set_status(self, status: str) -> None:
        """Set job status"""
        await self.__setitem__("_status", status)
    
    async def get_status(self) -> Optional[str]:
        """Get job status"""
        return await self.get("_status")


class RedisQueueManager:
    """Manages Redis-based job queue for node execution"""
    
    def __init__(self, redis_url: str = None, 
                 queue_name: str = "pf:queue", 
                 result_prefix: str = "pf:result:"):
        # Defensive programming: validate arguments
        if queue_name and not isinstance(queue_name, str):
            raise ValueError("queue_name must be a string")
        if result_prefix and not isinstance(result_prefix, str):
            raise ValueError("result_prefix must be a string")
        
        self.redis_url = redis_url or self._get_redis_url()
        
        # Defensive programming: ensure redis_url is valid
        if not self.redis_url:
            raise ValueError("Redis URL could not be determined from arguments or environment")
        
        self.queue_name = queue_name or "pf:queue"
        self.result_prefix = result_prefix or "pf:result:"
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=True)
    
    def _get_redis_url(self) -> str:
        """Get Redis URL from environment or default"""
        import os
        
        # Priority order for Redis URL detection
        redis_url = os.getenv('REDIS_URL')
        if redis_url and redis_url.strip():
            return redis_url.strip()
            
        redis_host = os.getenv('REDIS_HOST')
        if redis_host and redis_host.strip():
            redis_port = os.getenv('REDIS_PORT', '6379')
            if not redis_port or not redis_port.strip():
                redis_port = '6379'
            return f"redis://{redis_host.strip()}:{redis_port.strip()}"
            
        elasticache_endpoint = os.getenv('ELASTICACHE_ENDPOINT')
        if elasticache_endpoint and elasticache_endpoint.strip():
            return f"redis://{elasticache_endpoint.strip()}:6379"
            
        # Local development fallback
        return "redis://localhost:6379"
    
    def enqueue(self, message: QueueMessage) -> None:
        """Add a message to the queue"""
        json_msg = json.dumps(message.to_dict())
        self.r.rpush(self.queue_name, json_msg)
    
    def dequeue(self, timeout: int = 0) -> Optional[QueueMessage]:
        """Remove and return a message from the queue"""
        result = self.r.blpop(self.queue_name, timeout=timeout)
        if result:
            _, raw_msg = result
            data = json.loads(raw_msg)
            return QueueMessage.from_dict(data)
        return None
    
    def queue_size(self) -> int:
        """Get the current queue size"""
        return self.r.llen(self.queue_name)
    
    def create_job(self, start_node_name: str, params: Dict[str, Any] = None) -> str:
        """Create a new job and enqueue its first message"""
        job_id = str(uuid.uuid4())
        message = QueueMessage(
            job_id=job_id,
            node_name=start_node_name,
            params=params or {}
        )
        self.enqueue(message)
        return job_id
    
    def set_job_result(self, job_id: str, result: Any) -> None:
        """Store the final result for a job"""
        result_key = f"{self.result_prefix}{job_id}"
        self.r.set(result_key, json.dumps(result), ex=3600)
    
    def get_job_result(self, job_id: str) -> Any:
        """Get the final result for a job"""
        result_key = f"{self.result_prefix}{job_id}"
        result = self.r.get(result_key)
        return json.loads(result) if result else None


class AsyncRedisQueueManager:
    """Async version of RedisQueueManager"""
    
    def __init__(self, redis_url: str = None, 
                 queue_name: str = "pf:queue", 
                 result_prefix: str = "pf:result:"):
        # Defensive programming: validate arguments
        if queue_name and not isinstance(queue_name, str):
            raise ValueError("queue_name must be a string")
        if result_prefix and not isinstance(result_prefix, str):
            raise ValueError("result_prefix must be a string")
        
        self.redis_url = redis_url or self._get_redis_url()
        
        # Defensive programming: ensure redis_url is valid
        if not self.redis_url:
            raise ValueError("Redis URL could not be determined from arguments or environment")
        
        self.queue_name = queue_name or "pf:queue"
        self.result_prefix = result_prefix or "pf:result:"
        self.r = None
    
    def _get_redis_url(self) -> str:
        """Get Redis URL from environment or default"""
        import os
        
        # Priority order for Redis URL detection
        redis_url = os.getenv('REDIS_URL')
        if redis_url and redis_url.strip():
            return redis_url.strip()
            
        redis_host = os.getenv('REDIS_HOST')
        if redis_host and redis_host.strip():
            redis_port = os.getenv('REDIS_PORT', '6379')
            if not redis_port or not redis_port.strip():
                redis_port = '6379'
            return f"redis://{redis_host.strip()}:{redis_port.strip()}"
            
        elasticache_endpoint = os.getenv('ELASTICACHE_ENDPOINT')
        if elasticache_endpoint and elasticache_endpoint.strip():
            return f"redis://{elasticache_endpoint.strip()}:6379"
            
        # Local development fallback
        return "redis://localhost:6379"
    
    async def _get_redis(self):
        """Lazy initialization of Redis connection"""
        if self.r is None:
            self.r = aioredis.Redis.from_url(self.redis_url, decode_responses=True)
        return self.r
    
    async def enqueue(self, message: QueueMessage) -> None:
        """Add a message to the queue"""
        r = await self._get_redis()
        json_msg = json.dumps(message.to_dict())
        await r.rpush(self.queue_name, json_msg)
    
    async def dequeue(self, timeout: int = 0) -> Optional[QueueMessage]:
        """Remove and return a message from the queue"""
        r = await self._get_redis()
        result = await r.blpop(self.queue_name, timeout=timeout)
        if result:
            _, raw_msg = result
            data = json.loads(raw_msg)
            return QueueMessage.from_dict(data)
        return None
    
    async def queue_size(self) -> int:
        """Get the current queue size"""
        r = await self._get_redis()
        return await r.llen(self.queue_name)
    
    async def create_job(self, start_node_name: str, params: Dict[str, Any] = None) -> str:
        """Create a new job and enqueue its first message"""
        job_id = str(uuid.uuid4())
        message = QueueMessage(
            job_id=job_id,
            node_name=start_node_name,
            params=params or {}
        )
        await self.enqueue(message)
        return job_id
    
    async def set_job_result(self, job_id: str, result: Any) -> None:
        """Store the final result for a job"""
        r = await self._get_redis()
        result_key = f"{self.result_prefix}{job_id}"
        await r.set(result_key, json.dumps(result), ex=3600)
    
    async def get_job_result(self, job_id: str) -> Any:
        """Get the final result for a job"""
        r = await self._get_redis()
        result_key = f"{self.result_prefix}{job_id}"
        result = await r.get(result_key)
        return json.loads(result) if result else None


class RedisFlow(Flow):
    """Redis-backed flow that supports distributed execution"""
    
    def __init__(self, start=None, queue_manager: RedisQueueManager = None):
        super().__init__(start)
        self.queue_manager = queue_manager or RedisQueueManager()
    
    def run_job(self, job_id: str, params: Dict[str, Any] = None) -> Any:
        """Run a flow for a specific job_id using Redis shared store"""
        shared = RedisSharedStore(job_id)
        shared.set_status("running")
        
        try:
            result = self._run(shared)
            shared.set_status("completed")
            self.queue_manager.set_job_result(job_id, result)
            return result
        except Exception as e:
            shared.set_status("failed")
            shared["error"] = str(e)
            raise
    
    def enqueue_successor(self, job_id: str, current_node: BaseNode, action: str) -> None:
        """Enqueue the next node for execution"""
        next_node = self.get_next_node(current_node, action)
        if next_node:
            message = QueueMessage(
                job_id=job_id,
                node_name=next_node.__class__.__name__,
                action=action,
                params=next_node.params
            )
            self.queue_manager.enqueue(message)
    
    def create_and_run_job(self, params: Dict[str, Any] = None) -> str:
        """Create a new job and start execution"""
        if not self.start_node:
            raise ValueError("No start node defined")
        
        job_id = self.queue_manager.create_job(
            self.start_node.__class__.__name__, 
            params
        )
        return job_id


class AsyncRedisFlow(AsyncFlow):
    """Async Redis-backed flow that supports distributed execution"""
    
    def __init__(self, start=None, queue_manager: AsyncRedisQueueManager = None):
        super().__init__(start)
        self.queue_manager = queue_manager or AsyncRedisQueueManager()
    
    async def run_job(self, job_id: str, params: Dict[str, Any] = None) -> Any:
        """Run a flow for a specific job_id using Redis shared store"""
        shared = AsyncRedisSharedStore(job_id)
        await shared.set_status("running")
        
        try:
            result = await self._run_async(shared)
            await shared.set_status("completed")
            await self.queue_manager.set_job_result(job_id, result)
            return result
        except Exception as e:
            await shared.set_status("failed")
            await shared.__setitem__("error", str(e))
            raise
    
    async def enqueue_successor(self, job_id: str, current_node: BaseNode, action: str) -> None:
        """Enqueue the next node for execution"""
        next_node = self.get_next_node(current_node, action)
        if next_node:
            message = QueueMessage(
                job_id=job_id,
                node_name=next_node.__class__.__name__,
                action=action,
                params=next_node.params
            )
            await self.queue_manager.enqueue(message)
    
    async def create_and_run_job(self, params: Dict[str, Any] = None) -> str:
        """Create a new job and start execution"""
        if not self.start_node:
            raise ValueError("No start node defined")
        
        job_id = await self.queue_manager.create_job(
            self.start_node.__class__.__name__, 
            params
        )
        return job_id