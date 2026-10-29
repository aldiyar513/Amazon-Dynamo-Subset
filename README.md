# Amazon Dynamo Subset

A Dynamo-inspired distributed key-value store built in Python using the course-provided `dslabs` simulation framework.

The project supports replicated writes, read and write quorums, request timeouts, JSON persistence, gossip membership, and basic failure detection using ping/pong messages.

Nodes run inside a simulated network, where message delivery and network partitions can be controlled.

For documentation of the underlying framework, see [README-dslabs.md](README-dslabs.md).

## Setup

Clone the repository and enter its folder:

```bash
git clone https://github.com/aldiyar513/Amazon-Dynamo-Subset.git
cd Amazon-Dynamo-Subset
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows, activate it with:

```powershell
.venv\Scripts\Activate.ps1
```

Install the tools needed for tests and the notebook:

```bash
python -m pip install pytest jupyter ipykernel
```

Run the commands below from the repository's root folder.

## Run the tests

Run the Dynamo tests:

```bash
python -m pytest tests/test_dynamo.py -v
```

The tests exercise replication, quorums, timeouts, persistence, membership gossip, and failure detection.

## Run the demonstration notebook

Start Jupyter:

```bash
python -m jupyter notebook
```

Open the demonstration notebook in the `notebooks` folder and run its cells from top to bottom.

You can also open the notebook in VS Code and select the Python interpreter from `.venv` as its kernel.

The demonstration uses three nodes, A, B, and C. It builds on the same shopping-cart example to show writes, reads, network partitions, reconnection, persistence, and membership discovery.

Restart the kernel and run all cells to repeat the demonstration from a clean state.

## Create a three-node cluster

Run the following code in a notebook cell or a Python script in the repository root:

```python
from dslabs import SimNetwork, SimScheduler, partition
from dslabs.nodes.node_dynamo import NodeDynamo

scheduler = SimScheduler()
network = SimNetwork(
    scheduler,
    seed=7,
    latency_ms=(30, 80),
)

node_ids = ["A", "B", "C"]
nodes = {}

for node_id in node_ids:
    node = NodeDynamo(
        node_id=node_id,
        peers=node_ids,
        transport=network.endpoint(node_id),
        scheduler=scheduler,
        write_quorum=2,
        read_quorum=2,
    )
    nodes[node_id] = node
    network.register(node_id, node.on_message)
```

Each node has its own key-value store. With these settings, a write requires confirmation from two distinct nodes, and a read requires responses from two distinct nodes.

The scheduler controls simulated time. Messages are delivered when you advance it:

```python
scheduler.run_until(scheduler.now_ms() + 200)
```

`run_until()` takes an absolute simulated time in milliseconds. Adding a duration to `now_ms()` advances the simulation by that duration.

## Write data

Send a shopping-cart update to node A:

```python
write_id = nodes["A"].client_put("cart:42", {"book": 2})

print("Request:", write_id)
print("Pending:", write_id in nodes["A"].pending_writes)

scheduler.run_until(scheduler.now_ms() + 200)

print("Successful:", write_id in nodes["A"].successful_writes)
```

`client_put()` returns a request ID. Use that ID to check whether the write is pending, successful, or timed out.

The receiving node stores the value locally and sends it to its peers. The write is marked successful once enough distinct nodes have acknowledged it.

Inspect each replica:

```python
for node_id, node in nodes.items():
    print(node_id, node.store)
```

## Read data

Request the cart through node B:

```python
read_id = nodes["B"].client_get("cart:42")

scheduler.run_until(scheduler.now_ms() + 200)

if read_id in nodes["B"].completed_reads:
    print("Responses:", nodes["B"].completed_reads[read_id])
elif read_id in nodes["B"].timed_out_reads:
    print("Read timed out")
else:
    print("Read is still pending")
```

`client_get()` returns a request ID. A completed read contains the responses collected from individual nodes, for example:

```python
{"B": {"book": 2}, "A": {"book": 2}}
```

The read quorum counts responses from distinct nodes. The returned dictionary lets you inspect their values, including any differences.

## Introduce a network partition

Disconnect C from A and B:

```python
cut = partition({"C"})
network.add_rule(cut)
```

Write a new value through A:

```python
write_id = nodes["A"].client_put("cart:42", {"book": 3})

scheduler.run_until(scheduler.now_ms() + 200)

print("Successful:", write_id in nodes["A"].successful_writes)

for node_id, node in nodes.items():
    print(node_id, node.store)
```

A and B can still satisfy the write quorum of two. C retains the previous value because it cannot receive the update.

## Inspect failure detection and reconnection

While C remains disconnected, advance time so that the health checks can detect missing responses:

```python
scheduler.run_until(scheduler.now_ms() + 5000)

for node_id, node in nodes.items():
    print(node_id, "suspects:", sorted(node.suspected_nodes))
```

Nodes periodically send ping messages and respond with pong messages. A node suspects a peer when it has not heard from it within the configured timeout.

Restore communication:

```python
network.remove_rule(cut)
scheduler.run_until(scheduler.now_ms() + 1500)

for node_id, node in nodes.items():
    print(node_id, "suspects:", sorted(node.suspected_nodes))
```

Receiving messages from a peer updates its last-seen time and removes it from the local suspicion set. This demonstrates recovery of communication and failure-detection state.

## Observe a request timeout

Isolate A so that it cannot reach its write quorum:

```python
cut_a = partition({"A"})
network.add_rule(cut_a)

write_id = nodes["A"].client_put("cart:99", {"pen": 1})

scheduler.run_until(
    scheduler.now_ms() + nodes["A"].write_timeout_ms + 100
)

print("Timed out:", write_id in nodes["A"].timed_out_writes)
print("Pending:", write_id in nodes["A"].pending_writes)

network.remove_rule(cut_a)
```

A timeout means that the coordinator did not collect enough confirmations before the deadline. The local write still exists.

## Save and restore data

Give each node a separate `storage_path` to enable JSON persistence. Create the parent directory first.

This standalone example writes a cart and recreates the node using the same file:

```python
from pathlib import Path

data_dir = Path("demo_data")
data_dir.mkdir(exist_ok=True)
storage_path = str(data_dir / "P.json")

persistence_scheduler = SimScheduler()
persistence_network = SimNetwork(persistence_scheduler, seed=0)

node = NodeDynamo(
    node_id="P",
    peers=["P"],
    transport=persistence_network.endpoint("P"),
    scheduler=persistence_scheduler,
    write_quorum=1,
    storage_path=storage_path,
)

node.client_put("cart:42", {"book": 3})
print("Before restart:", node.store)

restart_scheduler = SimScheduler()
restart_network = SimNetwork(restart_scheduler, seed=0)

restored_node = NodeDynamo(
    node_id="P",
    peers=["P"],
    transport=restart_network.endpoint("P"),
    scheduler=restart_scheduler,
    write_quorum=1,
    storage_path=storage_path,
)

print("After restart:", restored_node.store)
```

Writes automatically save the store when `storage_path` is configured. Creating a node with an existing storage file loads its saved data.

## Inspect membership and node state

Nodes periodically exchange their known membership through gossip.

```python
for node_id, node in nodes.items():
    print(
        node_id,
        "members:", sorted(node.known_members),
        "suspected:", sorted(node.suspected_nodes),
        "store:", node.brief_state(),
    )
```

Additional information is available through:

| Attribute | What it shows |
|---|---|
| `node.log` | Writes applied by the node, in delivery order |
| `node.pending_writes` | Writes waiting for enough acknowledgements |
| `node.successful_writes` | Write IDs that reached quorum |
| `node.timed_out_writes` | Write IDs that timed out |
| `node.pending_reads` | Responses collected for unfinished reads |
| `node.completed_reads` | Responses for reads that reached quorum |
| `node.timed_out_reads` | Read IDs that timed out |
| `node.last_seen` | Last simulated time a message arrived from each peer |
| `network.trace` | Recorded network events |
| `network.stats` | Message counts |

## Configuration

Pass these options when constructing `NodeDynamo`:

| Option | Default | Purpose |
|---|---:|---|
| `write_quorum` | `2` | Number of distinct nodes needed to confirm a write |
| `read_quorum` | `2` | Number of distinct responses needed to complete a read |
| `write_timeout_ms` | `500` | Deadline for collecting write acknowledgements |
| `read_timeout_ms` | `500` | Deadline for collecting read responses |
| `gossip_interval_ms` | `1000` | Time between membership gossip rounds |
| `health_check_interval_ms` | `1000` | Time between health checks |
| `failure_timeout_ms` | `3000` | Silence duration used to suspect a peer |
| `storage_path` | `None` | JSON file used to save and load the node's store |

All time settings use simulated milliseconds.