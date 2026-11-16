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