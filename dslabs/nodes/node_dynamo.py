from dataclasses import dataclass, field
from typing import Any
import json
from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeDynamo:
    """
    Dynamo-inspired key-value store.
    """

    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    # Key-value store
    store: dict[str, Any] = field(default_factory=dict)
    # Append-only log of everything delivered, in delivery order
    log: list[tuple[str, Any]] = field(default_factory=list)

    # Number of distinct nodes required to confirm a write
    write_quorum: int = 2

    # Local counter used to create unique request IDs
    request_counter: int = 0

    # Request ID -> set of nodes that have acknowledged the write
    pending_writes: dict[str, set[str]] = field(default_factory=dict)

    successful_writes: set[str] = field(default_factory=set)
    # write time outs 
    write_timeout_ms: int = 500 
    # read time outs
    read_timeout_ms: int = 500 
    
    timed_out_writes: set[str] = field(default_factory=set)
    timed_out_reads: set[str] = field(default_factory=set)

    # Reading Writes 
    read_quorum: int = 2
    pending_reads: dict[str, dict[str, Any]] = field(default_factory=dict)
    completed_reads: dict[str, dict[str, Any]] = field(default_factory=dict)
    
    storage_path: str | None = None

    known_members: set[str] = field(default_factory=set)

    gossip_interval_ms: int = 1000

    last_seen: dict[str, int] = field(default_factory=dict)
    suspected_nodes: set[str] = field(default_factory=set)
    failure_timeout_ms: int = 3000

    health_check_interval_ms: int = 1000

    # Client-facing API
    def client_put(self, key: str, value: Any) -> str:
        # Register the write request with Node_ID and ID
        self.request_counter += 1
        request_id = f"{self.node_id}:{self.request_counter}"
        self.receive_and_replicate(request_id, key, value)
        return request_id

    def client_get(self, key: str) -> str:
        self.request_counter += 1
        request_id = f"{self.node_id}:{self.request_counter}"
        self.pending_reads[request_id] = {
                    self.node_id: self.store.get(key)
                }
        self.scheduler.call_later(
            self.read_timeout_ms,
            lambda: self.on_read_timeout(request_id),
        )
        # if we need only one node to confirm 
        if len(self.pending_reads[request_id]) >= self.read_quorum:
            self.completed_reads[request_id] = self.pending_reads[request_id]
            del self.pending_reads[request_id]

        # Send read requests to peers
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "read", "key": key, "request_id": request_id, "coordinator": self.node_id})
        return request_id

    # Node internals
    def receive_and_replicate(self, request_id: str, key: str, value: Any) -> None:
        # Write the Request then count it 
        self.receive(key, value)
        self.pending_writes[request_id] = {self.node_id}
        self.scheduler.call_later(
                    self.write_timeout_ms,
                    lambda: self.on_write_timeout(request_id),
                )
                
        if len(self.pending_writes[request_id]) >= self.write_quorum:
            del self.pending_writes[request_id]
            self.successful_writes.add(request_id)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value, "request_id": request_id, "coordinator": self.node_id})

    def receive(self, key: str, value: Any) -> None:
        # Received messages are delivered immediately
        self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))
        self.save_store()

    # Network handler
    def on_message(self, msg: Message) -> None:
        sender = msg.get("from")
        if sender is not None:
            self.mark_seen(sender)
        if msg["type"] == "replicate":
            self.receive(msg["key"], msg["value"])
            self.transport.send(msg["coordinator"], {"type": "ack", "request_id": msg["request_id"], "node_id": self.node_id})

        elif msg["type"] == "ack":
            if msg["request_id"] in self.pending_writes:
                self.pending_writes[msg["request_id"]].add(msg["node_id"])
                if len(self.pending_writes[msg["request_id"]]) >= self.write_quorum:
                    del self.pending_writes[msg["request_id"]]
                    self.successful_writes.add(msg["request_id"])
        elif msg["type"] == "read":
            # Just look up the value and send to the coordinator
            self.transport.send(msg["coordinator"], {"type": "read_response", "request_id": msg["request_id"], "node_id": self.node_id, "value": self.store.get(msg["key"])})
        elif msg["type"] == "read_response":
            if msg["request_id"] in self.pending_reads:
                self.pending_reads[msg["request_id"]][msg["node_id"]] = msg["value"]
                if len(self.pending_reads[msg["request_id"]]) >= self.read_quorum:
                    self.completed_reads[msg["request_id"]] = self.pending_reads[msg["request_id"]]
                    del self.pending_reads[msg["request_id"]]
        elif msg["type"] == "gossip":
            self.known_members.update(msg["members"])
        elif msg["type"] == "ping":
            self.transport.send(sender, {"type": "pong"})
        elif msg["type"] == "pong":
            pass  # Just mark_seen is enough
        else:
            raise ValueError(f"Unknown message type in {msg!r}")
        
    
    def on_write_timeout(self, request_id: str) -> None:
        if request_id in self.pending_writes:
            del self.pending_writes[request_id]
            self.timed_out_writes.add(request_id)

    def on_read_timeout(self, request_id: str) -> None:
        if request_id in self.pending_reads:
            del self.pending_reads[request_id]
            self.timed_out_reads.add(request_id)

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
    def save_store(self) -> None:
        # Write to the store path to the file as well 
        if self.storage_path:
            with open(self.storage_path, "w", encoding="utf-8") as f:
                json.dump(self.store, f)
        else: 
            return 
    def load_store(self) -> None:
        # Read from the file 
        if self.storage_path:
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    self.store = json.load(f)
            except FileNotFoundError:
                pass
        else: 
            return
    def gossip_once(self) -> None:
        import random 
        members = sorted(list(self.known_members))
        recipients = [member for member in members if member != self.node_id]
        if recipients:
            # Pick a random member to gossip to
            recipient = random.choice(recipients)
            self.transport.send(recipient, {"type": "gossip", "members": members})
        else:
            return 
    def on_gossip_tick(self) -> None:
        self.gossip_once()
        self.scheduler.call_later(self.gossip_interval_ms, self.on_gossip_tick)

    def on_health_check_tick(self) -> None:
        time = self.scheduler.now_ms()
        for peer in sorted(self.known_members):
            if peer == self.node_id:
                continue
            last_seen = self.last_seen.setdefault(peer, time)
            if time - last_seen >= self.failure_timeout_ms:
                self.suspected_nodes.add(peer)
            self.transport.send(peer, {"type": "ping"})
        self.scheduler.call_later(
            self.health_check_interval_ms,
            self.on_health_check_tick,
        )

    def mark_seen(self, node_id: str) -> None:
        self.last_seen[node_id] = self.scheduler.now_ms()
        self.suspected_nodes.discard(node_id)

    def __post_init__(self) -> None:
        self.load_store()
        self.known_members.add(self.node_id)
        self.known_members.update(self.peers)
        self.scheduler.call_later(self.gossip_interval_ms, self.on_gossip_tick)
        self.scheduler.call_later(self.health_check_interval_ms, self.on_health_check_tick)
