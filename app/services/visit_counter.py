import logging
from typing import Dict, Any
from ..core.redis_manager import RedisManager
import asyncio

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

            # Swap the current buffer with an empty one
            buffer_to_flush = self.write_buffer
            self.write_buffer = {}

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


    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and which Redis served the request
        """
        value, served_via = await self.redis_manager.increment(f"visits:{page_id}")
        return {
            "visits": value,
            "served_via": served_via
        }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and which Redis served the request
        """
        value, served_via = await self.redis_manager.get(f"visits:{page_id}")
        return {
            "visits": value if value is not None else 0,
            "served_via": served_via
        }