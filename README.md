# Amazon Dynamo Subset

A Python implementation of a Dynamo-inspired distributed key-value store for shopping-cart data. Built as a Distributed Systems course project using the `dslabs` simulation framework.

Nodes maintain separate stores and communicate through simulated network messages. The system runs in one Python process, with a scheduler controlling message delivery and simulated time.

## Current functionality

| Feature | Description |
|---|---|
| Replication | Any node can accept a write, store it locally, and send it to its configured peers. |
| Write quorum | A write succeeds after the configured number of distinct nodes acknowledge it. |
| Read quorum | A read completes after collecting responses from the configured number of distinct nodes. |
| Timeouts | Requests that do not reach quorum within the configured period are marked as timed out. |
| Persistence | Nodes can save their stores to individual JSON files and restore them when recreated. |
| Gossip membership | Nodes periodically share membership information with a randomly selected member. |
| Failure detection | Periodic ping/pong messages and last-seen timestamps identify suspected unreachable nodes. Incoming messages clear suspicion. |
| State inspection | Node data, membership, suspected nodes, and request outcomes can be inspected in the notebook demonstration. |

## Setup

Use Python 3.10 or later.

```bash
git clone https://github.com/aldiyar513/Amazon-Dynamo-Subset.git
cd Amazon-Dynamo-Subset

python3 -m venv .venv
source .venv/bin/activate
python -m pip install pytest ipykernel
```

Run commands from the repository root.

## Run the tests

```bash
python -m pytest tests/test_dynamo.py -v
```

The Dynamo tests cover quorum operations, timeouts, persistence, membership discovery, network partitions, and failure detection.


## Main implementation files

| Path | Purpose |
|---|---|
| `dslabs/nodes/node_dynamo.py` | Dynamo-inspired node implementation. |
| `tests/test_dynamo.py` | Tests for the implemented features and failure scenarios. |
| `notebooks/progress_demo.ipynb` | Step-by-step demonstration with visible node state. |

Other node implementations and simulator utilities belong to the underlying course framework.

## Using the client API

`client_put(key, value)` returns a request ID. The coordinating node records the request in `pending_writes`, then moves it to either `successful_writes` or `timed_out_writes`.

`client_get(key)` also returns a request ID. Once enough responses arrive, `completed_reads[request_id]` contains a mapping from responding node IDs to their returned values. Reads that do not reach quorum are recorded in `timed_out_reads`.

Both operations count the coordinating node's local contribution toward quorum. With a threshold of one, an operation can complete immediately.

To enable persistence, provide a different `storage_path` for each node. Its parent directory must already exist. Nodes without a storage path operate in memory.


## Acknowledgments

This project builds on the course-provided `dslabs` framework, including its simulated network, scheduler, and tracing utilities. Its design is inspired by Amazon's Dynamo system.