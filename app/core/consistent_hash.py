import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100): 
        # TODO: Initialize the hash ring with virtual nodes
        # 1. For each physical node, create virtual_nodes number of virtual nodes
        # 2. Calculate hash for each virtual node and map it to the physical node
        # 3. Store the mapping in hash_ring and maintain sorted_keys
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []

        # Initialize hash ring with provided nodes
        for node in nodes:
            self.add_node(node)
        
    def _hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        # TODO: Implement adding a new node
        # 1. Create virtual nodes for the new physical node
        # 2. Update hash_ring and sorted_keys

        # Create virtual nodes
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}_{i}"
            hash_key = self._hash(virtual_node_key)

            # Add to hash ring
            self.hash_ring[hash_key] = node

            # Insert into sorted keys maintaining order
            insert_pos = bisect(self.sorted_keys, hash_key)
            self.sorted_keys.insert(insert_pos, hash_key)

    def remove_node(self, node: str) -> None:
        # TODO: Implement removing a node
        # 1. Remove all virtual nodes for the given physical node
        # 2. Update hash_ring and sorted_keys
        
        # Remove all virtual nodes for the given physical node
        keys_to_remove = []

        # Find all keys belonging to this node
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}_{i}"
            hash_key = self._hash(virtual_node_key)
            keys_to_remove.append(hash_key)
        
        # Remove from hash ring and sorted keys
        for key in keys_to_remove:
            if key in self.hash_ring:
                del self.hash_ring[key]
                self.sorted_keys.remove(key)

    def get_node(self, key: str) -> str:
        # TODO: Implement node lookup
        # 1. Calculate hash of the key
        # 2. Find the first node in the ring that comes after the key's hash
        # 3. If no such node exists, wrap around to the first node
        if not self.sorted_keys:
            raise ValueError("Hash ring is empty")

        # Get hash of the key
        hash_key = self._hash(key)

        # Find the first node in the ring that comes after the key's hash
        pos = bisect(self.sorted_keys, hash_key)

        # If we're past the end, wrap around to the first node
        if pos >= len(self.sorted_keys):
            pos = 0
        
        # Return the physical node responsible for this hash
        return self.hash_ring[self.sorted_keys[pos]]
    