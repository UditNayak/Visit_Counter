import redis
from app.core.config import settings

class RedisManager:
    def __init__(self):
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        redis_node = redis_nodes[0]

        if "redis://" in redis_node:
            redis_node = redis_node.replace("redis://", "")

        host, port = redis_node.split(":")

        self.redis_client = redis.Redis(host=host, port=int(port), db=settings.REDIS_DB,
                                        password=settings.REDIS_PASSWORD)

    async def increment(self, key: str, amount: int = 1) -> int:
        return self.redis_client.incrby(key, amount)

    async def get(self, key: str) -> int:
        value = self.redis_client.get(key)
        return int(value) if value else 0