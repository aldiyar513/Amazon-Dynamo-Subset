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

    # Client-facing API
    def client_put(self, key: str, value: Any) -> str:
        # Register the write request with Node_ID and ID
        self.request_counter += 1
        request_id = f"{self.node_id}:{self.request_counter}"
        self.receive_and_replicate(request_id, key, value)
        return request_id

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def receive_and_replicate(self, request_id: str, key: str, value: Any) -> None:
        # Write the Request then count it 
        self.receive(key, value)
        self.pending_writes[request_id] = {self.node_id}
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
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
    