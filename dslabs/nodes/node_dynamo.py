from dataclasses import dataclass, field
from typing import Any
import json
from dslabs.protocols import Message, Scheduler, Transport
from dslabs.vector_clock import increment_clock, compare_clocks, merge_clocks


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
    completed_reads: dict[str, list[tuple[Any, dict[str, int]]]] = field(default_factory=dict)

    storage_path: str | None = None

    known_members: set[str] = field(default_factory=set)

    gossip_interval_ms: int = 1000

    last_seen: dict[str, int] = field(default_factory=dict)
    suspected_nodes: set[str] = field(default_factory=set)
    failure_timeout_ms: int = 3000

    health_check_interval_ms: int = 1000
    clocks: dict[str, dict[str, int]] = field(default_factory=dict) 

    versions: dict[str, list[tuple[Any, dict[str, int]]]] = field(default_factory=dict) 

    # Client-facing API
    def client_put(self, key: str, value: Any, context: list[dict[str, int]] | None = None) -> str:
        self.request_counter += 1
        request_id = f"{self.node_id}:{self.request_counter}"

        if context is None:
            base_clock = self.clocks.get(key, {}).copy()
        else:
            base_clock = merge_clocks(context)

        for _, sibling_clock in self.versions.get(key, []):
            base_clock[self.node_id] = max(
                base_clock.get(self.node_id, 0),
                sibling_clock.get(self.node_id, 0),
            )

        new_clock = increment_clock(base_clock, self.node_id)
        self.receive_and_replicate(request_id, key, value, new_clock)
        return request_id

    def client_get(self, key: str) -> str:
        self.request_counter += 1
        request_id = f"{self.node_id}:{self.request_counter}"
        self.pending_reads[request_id] = {
                self.node_id: self.versions.get(key, []).copy()
            }
        self.scheduler.call_later(
            self.read_timeout_ms,
            lambda: self.on_read_timeout(request_id),
        )
        # if we need only one node to confirm 
        if len(self.pending_reads[request_id]) >= self.read_quorum:
            self.completed_reads[request_id] = self._merge_read_versions(self.pending_reads[request_id])
            del self.pending_reads[request_id]

        # Send read requests to peers
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "read", "key": key, "request_id": request_id, "coordinator": self.node_id})
        return request_id

    # Node internals
    def receive_and_replicate(self, request_id: str, key: str, value: Any, clock: dict[str, int]) -> None:
        # Write the Request then count it
        self.receive(key, value, clock)
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
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value, "request_id": request_id, "coordinator": self.node_id, "clock": clock})

    def receive(self, key: str, value: Any, clock: dict[str, int]) -> None:
        siblings = self.versions.get(key, [])
        survivors: list[tuple[Any, dict[str, int]]] = []

        for sibling_value, sibling_clock in siblings:
            result = compare_clocks(clock, sibling_clock)

            if result in ("before", "equal"):
                return

            if result == "concurrent":
                survivors.append((sibling_value, sibling_clock))

            
        survivors.append((value, clock.copy()))
        self.versions[key] = survivors
        self.deliver(key, value, clock)
        
    def deliver(self, key: str, value: Any, clock: dict[str, int]) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.clocks[key] = clock.copy()
        self.log.append((key, value))
        self.save_store()

    def _merge_read_versions(self, responses: dict[str, list[tuple[Any, dict[str, int]]]]) -> list[tuple[Any, dict[str, int]]]:
        survivors: list[tuple[Any, dict[str, int]]] = []
        for versions in responses.values():
            for value, clock in versions:
                remaining: list[tuple[Any, dict[str, int]]] = []

                for stored_value, stored_clock in survivors:
                    result = compare_clocks(clock, stored_clock)

                    if result in ("before", "equal"):
                        break  # Skip this incoming version.

                    if result == "concurrent":
                        remaining.append((stored_value, stored_clock))

                    # If result is "after", leave the older version out.
                else:
                    remaining.append((value, clock.copy()))
                    survivors = remaining

        return survivors

    # Network handler
    def on_message(self, msg: Message) -> None:
        sender = msg.get("from")
        if sender is not None:
            self.mark_seen(sender)
        if msg["type"] == "replicate":
            self.receive(msg["key"], msg["value"], msg["clock"])
            self.transport.send(msg["coordinator"], {"type": "ack", "request_id": msg["request_id"], "node_id": self.node_id})

        elif msg["type"] == "ack":
            if msg["request_id"] in self.pending_writes:
                self.pending_writes[msg["request_id"]].add(msg["node_id"])
                if len(self.pending_writes[msg["request_id"]]) >= self.write_quorum:
                    del self.pending_writes[msg["request_id"]]
                    self.successful_writes.add(msg["request_id"])
        elif msg["type"] == "read":
            # Just look up the value and send to the coordinator
            self.transport.send(msg["coordinator"], {"type": "read_response", "request_id": msg["request_id"], "node_id": self.node_id, "value": self.versions.get(msg["key"], []).copy()})
        elif msg["type"] == "read_response":
            request_id = msg["request_id"]

            if request_id in self.pending_reads:
                responses = self.pending_reads[request_id]
                responses[msg["node_id"]] = msg["value"]

                if len(responses) >= self.read_quorum:
                    self.completed_reads[request_id] = self._merge_read_versions(responses)
                    del self.pending_reads[request_id]
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
                json.dump({ "store": self.store, "versions": self.versions,}, f)
        else: 
            return 
    def load_store(self) -> None:
        # Read from the file 
        if self.storage_path:
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.store = data["store"]
                    self.versions = {
                        key: [(value, clock) for value, clock in siblings]
                        for key, siblings in data["versions"].items()
                    }
                    self.clocks = {
                        key: siblings[-1][1].copy()
                        for key, siblings in self.versions.items()
                        if siblings
                    }
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
