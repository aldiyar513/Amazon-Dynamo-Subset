import hashlib
def hash_position(text: str) -> int:
    encoded = text.encode("utf-8")
    hsh = hashlib.sha256(encoded).hexdigest()
    return int(hsh, 16) % (2**32)

def build_ring(node_ids: list[str]) -> list[tuple[int, str]]: 
    return sorted((hash_position(node_id), node_id) for node_id in node_ids)

def find_owner(key: str, ring: list[tuple[int, str]]) -> str:
    if not ring:
        raise ValueError("Cannot find an owner on an empty ring")

    key_hash = hash_position(key)

    for node_hash, node_id in ring:
        if key_hash <= node_hash:
            return node_id

    return ring[0][1]  # Wrap around to the first node.

def select_replicas(key: str, ring: list[tuple[int, str]], replication_factor: int) -> list[str]:
    if not ring:
        raise ValueError("Cannot select replicas from an empty ring")

    if replication_factor < 1:
        raise ValueError("Replication factor must be at least 1")

    if replication_factor > len(ring):
        raise ValueError("Replication factor cannot exceed the number of nodes")

    key_hash = hash_position(key)
    start_index = 0

    for i, (node_hash, node_id) in enumerate(ring):
        if key_hash <= node_hash:
            start_index = i
            break

    replicas = []
    for j in range(replication_factor):
        replicas.append(ring[(start_index + j) % len(ring)][1])
    return replicas

def select_fallback(key: str, ring: list[tuple[int, str]], home_replicas: list[str], suspected_nodes: set[str],) -> str | None:
    for node in select_replicas(key, ring, len(ring)):
        if node not in home_replicas and node not in suspected_nodes:
            return node
    return None

    