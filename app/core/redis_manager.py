import redis
import logging
from typing import Dict, Optional, Tuple
import asyncio
from .consistent_hash import ConsistentHash
from .config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        """Initialize Redis connections and consistent hashing"""
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from settings
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
        # Initialize Redis clients using correct host ports (7070, 7071)
        for node in redis_nodes:
            try:
                # Remove the "redis://" prefix for clean parsing
                host, port = node.replace("redis://", "").split(":")
                
                self.redis_clients[node] = redis.Redis(
                    host=host,
                    port=6379,
                    db=settings.REDIS_DB,
                    password=settings.REDIS_PASSWORD,
                    decode_responses=True
                )
                logger.info(f"Connected to Redis node at {host}:{port}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis node {node}: {e}")

    async def get_connection(self, key: str) -> Tuple[redis.Redis, str]:
        """Get the correct Redis connection for a given key using consistent hashing"""
        node = self.consistent_hash.get_node(key)
        
        if node not in self.redis_clients:
            raise ValueError(f"No Redis client found for node: {node}")
        
        return self.redis_clients[node], self._map_node_to_identifier(node)

    async def increment(self, key: str, amount: int = 1) -> Tuple[int, str]:
        """Increment a counter in Redis"""
        for _ in range(3):  # Retry up to 3 times
            try:
                client, node_identifier = await self.get_connection(key)
                value = client.incrby(key, amount)
                return value, node_identifier
            except redis.ConnectionError as e:
                logger.warning(f"Redis connection error: {e}. Retrying...")
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Failed to increment counter for key {key}: {e}")
                break
        raise RuntimeError(f"Failed to increment counter for key {key} after 3 retries.")

    async def get(self, key: str) -> Tuple[Optional[int], str]:
        """Get the value of a key from Redis"""
        for _ in range(3):
            try:
                client, node_identifier = await self.get_connection(key)
                value = client.get(key)
                return int(value) if value else None, node_identifier
            except redis.ConnectionError as e:
                logger.warning(f"Redis connection error: {e}. Retrying...")
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Failed to get value for key {key}: {e}")
                break
        raise RuntimeError(f"Failed to get value for key {key} after 3 retries.")

    def _map_node_to_identifier(self, node: str) -> str:
        """Map node URLs to response identifiers"""
        if "7070" in node:
            return "redis_7070"
        elif "7071" in node:
            return "redis_7071"
        return "unknown"
