import asyncio
from datetime import datetime
from typing import Dict
from app.core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self, ttl: int = 5):
        self.redis_manager = RedisManager()
        self.lock = asyncio.Lock()
        self.cache: Dict[str, Dict[str, int]] = {}  # {page_id: {"count": value, "expiry": timestamp}}
        self.cache_ttl = ttl # Cache expiry in seconds (5 seconds by default)

    async def increment_visit(self, page_id: str) -> None:
        # Write-Through Strategy
        async with self.lock:
            new_count = await self.redis_manager.increment(page_id)
            self.cache[page_id] = {"count": new_count, "expiry": datetime.now().timestamp() + self.cache_ttl}

    async def get_visit_count(self, page_id: str) -> Dict[str, str]:
        # Cache-Aside Strategy
        async with self.lock:
            current_time = datetime.now().timestamp()

            # Serve from in-memory cache if valid
            if page_id in self.cache and self.cache[page_id]["expiry"] > current_time:
                return {"visits": self.cache[page_id]["count"], "served_via": "in_memory"}
            
            # Fetch from Redis if cache miss or expired
            count = await self.redis_manager.get(page_id)
            count = int(count) if count is not None else 0

            # Store in cache with new expiry timestamp
            self.cache[page_id] = {"count": count, "expiry": current_time + self.cache_ttl}

            return {"visits": count, "served_via": "redis"}
