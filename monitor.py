#!/usr/bin/env python3
"""
Simple monitor script for Docker Compose
"""
import time
import sys
from pocketflow import REDIS_AVAILABLE

if not REDIS_AVAILABLE:
    print("Redis not available. Exiting...")
    sys.exit(1)

from pocketflow import RedisQueueManager

def main():
    qm = RedisQueueManager(redis_url='redis://redis:6379')
    print('Monitor started')
    
    while True:
        try:
            queue_size = qm.queue_size()
            print(f'Queue size: {queue_size}')
            time.sleep(10)
        except Exception as e:
            print(f'Monitor error: {e}')
            time.sleep(5)

if __name__ == "__main__":
    main()