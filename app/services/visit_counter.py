import asyncio
from app.core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        self.redis_manager = RedisManager()
        self.lock = asyncio.Lock()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        async with self.lock:
            await self.redis_manager.increment(page_id)

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        async with self.lock:
            return await self.redis_manager.get(page_id)
