from dslabs import SimNetwork, SimScheduler
from dslabs.network import partition
from dslabs.nodes.node_dynamo import NodeDynamo


def test_write_reaches_quorum():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))

    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # Client sends a write to node A
    request_id = nodes["A"].client_put("cart:42", {"book": 2})
    assert request_id in nodes["A"].pending_writes
    assert request_id not in nodes["A"].successful_writes
    scheduler.run_until(1000) 
    assert request_id not in nodes["A"].pending_writes
    assert request_id in nodes["A"].successful_writes

    assert nodes["B"].store["cart:42"] == {"book": 2}
    assert nodes["C"].store["cart:42"] == {"book": 2}


def test_write_succeeds_with_one_replica_disconnected():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))

    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # Client sends a write to node A
    network.add_rule(partition({"C"}))
    request_id = nodes["A"].client_put("cart:42", {"book": 2})
    assert request_id in nodes["A"].pending_writes
    assert request_id not in nodes["A"].successful_writes
    scheduler.run_until(1000)
    assert request_id not in nodes["A"].pending_writes
    assert request_id in nodes["A"].successful_writes
    assert nodes["A"].store["cart:42"] == {"book": 2}
    assert nodes["B"].store["cart:42"] == {"book": 2}
    assert "cart:42" not in nodes["C"].store


def test_write_quorum_one_succeeds_immediately():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))

    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            write_quorum=1,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # Client sends a write to node A
    request_id = nodes["A"].client_put("cart:42", {"book": 2})
    assert request_id not in nodes["A"].pending_writes
    assert request_id in nodes["A"].successful_writes
    assert nodes["A"].store["cart:42"] == {"book": 2}
    
    scheduler.run_until(1000)

    assert nodes["B"].store["cart:42"] == {"book": 2}
    assert nodes["C"].store["cart:42"] == {"book": 2}

def test_write_times_out_when_quorum_unreachable():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))

    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            write_quorum=2,
            write_timeout_ms=500,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # A cannot communicate with B or C
    network.add_rule(partition({"A"}))

    request_id = nodes["A"].client_put("cart:42", {"book": 2})

    # Before the deadline, the write is still pending
    scheduler.run_until(499)
    assert nodes["A"].pending_writes[request_id] == {"A"}
    assert request_id not in nodes["A"].timed_out_writes
    assert request_id not in nodes["A"].successful_writes

    # At the deadline, the write times out
    scheduler.run_until(500)
    assert request_id in nodes["A"].timed_out_writes
    assert request_id not in nodes["A"].pending_writes
    assert request_id not in nodes["A"].successful_writes

    # A timeout does not undo the local write
    assert nodes["A"].store["cart:42"] == {"book": 2}
    assert "cart:42" not in nodes["B"].store
    assert "cart:42" not in nodes["C"].store

def make_read_cluster(read_quorum=2):
    scheduler = SimScheduler()
    network = SimNetwork(
        scheduler, seed=0, latency_ms=(30, 80)
    )
    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            read_quorum=read_quorum,
            read_timeout_ms=500,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    return scheduler, network, nodes


def test_read_reaches_quorum():
    scheduler, network, nodes = make_read_cluster()

    # First replicate the cart to all nodes
    nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(1000)

    request_id = nodes["A"].client_get("cart:42")

    # Only A's local response exists initially
    assert nodes["A"].pending_reads[request_id] == {
        "A": {"book": 2}
    }
    assert request_id not in nodes["A"].completed_reads

    scheduler.run_until(2000)

    responses = nodes["A"].completed_reads[request_id]
    assert len(responses) == 2
    assert "A" in responses
    assert all(value == {"book": 2} for value in responses.values())
    assert request_id not in nodes["A"].pending_reads
    assert request_id not in nodes["A"].timed_out_reads


def test_read_quorum_one_succeeds_immediately():
    scheduler, network, nodes = make_read_cluster(read_quorum=1)

    nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(1000)

    network.add_rule(partition({"A"}))
    request_id = nodes["A"].client_get("cart:42")

    # Completes without running the scheduler again
    assert nodes["A"].completed_reads[request_id] == {
        "A": {"book": 2}
    }
    assert request_id not in nodes["A"].pending_reads

    scheduler.run_until(2000)
    assert request_id not in nodes["A"].timed_out_reads


def test_read_times_out_when_quorum_unreachable():
    scheduler, network, nodes = make_read_cluster()

    nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(1000)

    network.add_rule(partition({"A"}))
    request_id = nodes["A"].client_get("cart:42")

    # Read started at 1000 ms, so its deadline is 1500 ms
    scheduler.run_until(1499)
    assert nodes["A"].pending_reads[request_id] == {
        "A": {"book": 2}
    }
    assert request_id not in nodes["A"].timed_out_reads
    assert request_id not in nodes["A"].completed_reads

    scheduler.run_until(1500)
    assert request_id in nodes["A"].timed_out_reads
    assert request_id not in nodes["A"].pending_reads
    assert request_id not in nodes["A"].completed_reads

    # Reading and timing out do not change stored data
    assert nodes["A"].store["cart:42"] == {"book": 2}


def test_store_survives_node_restart(tmp_path):
    storage_path = str(tmp_path / "A.json")

    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0)

    node = NodeDynamo(
        node_id="A",
        peers=["A"],
        transport=network.endpoint("A"),
        scheduler=scheduler,
        write_quorum=1,
        storage_path=storage_path,
    )

    assert node.store == {}

    request_id = node.client_put("cart:42", {"book": 2})

    assert request_id in node.successful_writes
    assert (tmp_path / "A.json").exists()

    # Restart the node and check that the store is loaded from disk
    new_scheduler = SimScheduler()
    new_network = SimNetwork(new_scheduler, seed=0)

    restarted_node = NodeDynamo(
        node_id="A",
        peers=["A"],
        transport=new_network.endpoint("A"),
        scheduler=new_scheduler,
        write_quorum=1,
        storage_path=storage_path,
    )

    assert restarted_node.store == {"cart:42": {"book": 2}}