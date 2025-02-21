import asyncio
from collections import defaultdict

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with an in-memory defaultdict"""
        self.visit_counts: defaultdict[str, int] = defaultdict(int)
        self.lock = asyncio.Lock()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        async with self.lock:
            self.visit_counts[page_id] += 1

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        async with self.lock:
            return self.visit_counts[page_id]
