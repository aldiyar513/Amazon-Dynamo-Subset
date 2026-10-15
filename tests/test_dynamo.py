from dslabs import SimNetwork, SimScheduler
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