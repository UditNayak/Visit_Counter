import asyncio
from datetime import datetime
import logging
from typing import Dict
from app.core.redis_manager import RedisManager

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class VisitCounterService:
    def __init__(self, ttl: int = 5, flush_interval: int = 30):
        self.redis_manager = RedisManager()
        self.lock = asyncio.Lock()
        self.read_cache: Dict[str, Dict[str, int]] = {}  # {page_id: {"count": value, "expiry": timestamp}}
        self.cache_ttl = ttl # Cache expiry in seconds (5 seconds by default)

        # Buffer for write operations
        self.write_buffer: Dict[str, int] = {}  # {page_id: pending_count}
        self.flush_interval = flush_interval

        # Start the background flush task
        self.flush_task = asyncio.create_task(self._periodic_flush())
    
    async def _periodic_flush(self) -> None:
        """Background task that periodically flushes the write buffer to Redis."""
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_buffer()
                logger.info("Now periodic flush will sleep for 30 seconds")
            except Exception as e:
                print(f"Error in periodic_flush: {e}")
    
    async def _flush_buffer(self) -> None:
        """Flush all pending writes from the buffer to Redis."""
        logger.info("Buffer flush initiated")
        async with self.lock:
            if not self.write_buffer:
                logger.info("Buffer is empty, returning")
                return

            # Create a copy of the current buffer and clear it
            buffer_to_flush = self.write_buffer.copy()
            self.write_buffer.clear()

            # Update Redis for each page_id
            for page_id, count in buffer_to_flush.items():
                try:
                    await self.redis_manager.increment(page_id, count)
                    # Invalidate the read cache entry for this page
                    self.read_cache.pop(page_id, None)
                except Exception as e:
                    print(f"Error flushing buffer for {page_id}: {e}")
                    # On error, add counts back to buffer
                    async with self.lock:
                        self.write_buffer[page_id] = (self.write_buffer.get(page_id, 0) + count)
            logger.info("Buffer flush completed")


    async def increment_visit(self, page_id: str) -> None:
        """Increment visit count in the write buffer."""
        async with self.lock:
            self.write_buffer[page_id] = self.write_buffer.get(page_id, 0) + 1

    async def get_visit_count(self, page_id: str) -> Dict[str, str]:
        """Get total visit count combining Redis and pending writes."""
        current_time = datetime.now().timestamp()

        # First check the read cache
        async with self.lock:
            # Serve from in-memory cache if valid
            if page_id in self.read_cache and self.read_cache[page_id]["expiry"] > current_time:
               cached_count = self.read_cache[page_id]["count"]
               pending_count = self.write_buffer.get(page_id, 0)
               return {"visits": cached_count + pending_count, "served_via": "in_memory", "pending_writes": pending_count, "cached_count": cached_count}
            
        # On cache miss, flush the buffer and get fresh count from Redis
        logger.info("Cache miss, flushing buffer")
        await self._flush_buffer()

        # Get the latest count from Redis
        redis_count = await self.redis_manager.get(page_id)

        # Update the read cache
        async with self.lock:
            self.read_cache[page_id] = {"count": redis_count, "expiry": current_time + self.cache_ttl}

            # Add any new pending writes that accumulated during the flush
            pending_count = self.write_buffer.get(page_id, 0)

            return {"visits": redis_count + pending_count, "served_via": "redis", "pending_writes": pending_count}
        
    
    async def cleanup(self) -> None:
        """Cleanup method to be called when shutting down the service."""
        # Cancel the periodic flush task
        logger.info("Cleaning up VisitCounterService")
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining writes
        await self._flush_buffer()
