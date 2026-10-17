from dataclasses import dataclass, field
from typing import Any

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

    # Network handler
    def on_message(self, msg: Message) -> None:
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
    