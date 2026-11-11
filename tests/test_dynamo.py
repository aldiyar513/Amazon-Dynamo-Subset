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
    version = ({"book": 2}, {"A": 1})
    assert nodes["A"].pending_reads[request_id] == {"A": [version]}
    assert request_id not in nodes["A"].completed_reads

    scheduler.run_until(2000)

    assert nodes["A"].completed_reads[request_id] == [version]
    assert request_id not in nodes["A"].pending_reads
    assert request_id not in nodes["A"].timed_out_reads


def test_read_quorum_one_succeeds_immediately():
    scheduler, network, nodes = make_read_cluster(read_quorum=1)

    nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(1000)

    network.add_rule(partition({"A"}))
    request_id = nodes["A"].client_get("cart:42")

    # Completes without running the scheduler again
    assert nodes["A"].completed_reads[request_id] == [({"book": 2}, {"A": 1})]
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
    assert nodes["A"].pending_reads[request_id] == {"A": [({"book": 2}, {"A": 1})]}
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


def test_replica_restores_data_after_restart(tmp_path):
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))

    node_ids = ["A", "B"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            write_quorum=2,
            storage_path=str(tmp_path / f"{node_id}.json"),
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # Only A receives a client write. B receives it through replication.
    request_id = nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(1000)

    assert request_id in nodes["A"].successful_writes
    assert nodes["B"].store == {"cart:42": {"book": 2}}
    assert (tmp_path / "B.json").exists()

    # Recreate B in a fresh simulator, using its saved file.
    new_scheduler = SimScheduler()
    new_network = SimNetwork(new_scheduler, seed=0)

    restarted_node = NodeDynamo(
        node_id="B",
        peers=node_ids,
        transport=new_network.endpoint("B"),
        scheduler=new_scheduler,
        storage_path=str(tmp_path / "B.json"),
    )

    # No messages have been delivered in this new simulator.
    # B must have restored this data from its file.
    assert restarted_node.store == {"cart:42": {"book": 2}}


def test_gossip_merges_known_members():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0)

    node = NodeDynamo(
        node_id="A",
        peers=["B"],
        transport=network.endpoint("A"),
        scheduler=scheduler,
    )

    assert node.known_members == {"A", "B"}

    node.on_message({
        "type": "gossip",
        "members": ["B", "C"],
    })

    assert node.known_members == {"A", "B", "C"}

def test_periodic_gossip_spreads_membership(monkeypatch):
    monkeypatch.setattr("random.choice", lambda candidates: candidates[0])

    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(10, 10))

    initial_peers = {
        "A": ["B"],
        "B": ["C"],
        "C": ["A"],
    }
    nodes = {}

    for node_id, peers in initial_peers.items():
        node = NodeDynamo(
            node_id=node_id,
            peers=peers,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            gossip_interval_ms=1000,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    def gossip_send_count():
        return sum(
            event.kind == "send"
            and event.msg.get("type") == "gossip"
            for event in network.trace
        )

    # Before the first tick, nobody has learned anything new.
    scheduler.run_until(999)

    assert nodes["A"].known_members == {"A", "B"}
    assert nodes["B"].known_members == {"B", "C"}
    assert nodes["C"].known_members == {"A", "C"}
    assert gossip_send_count() == 0

    # First gossip messages arrive at 1010 ms.
    scheduler.run_until(1010)

    for node in nodes.values():
        assert node.known_members == {"A", "B", "C"}

    sent_after_first_round = gossip_send_count()
    assert sent_after_first_round == 3

    # Each node sends another gossip message in the second round.
    scheduler.run_until(2010)

    assert gossip_send_count() == sent_after_first_round + 3

def test_gossip_with_no_other_members():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0)

    node = NodeDynamo(
        node_id="A",
        peers=[],
        transport=network.endpoint("A"),
        scheduler=scheduler,
        gossip_interval_ms=1000,
    )
    network.register("A", node.on_message)

    scheduler.run_until(3000)

    assert node.known_members == {"A"}
    assert network.stats["sent"] == 0


def test_gossip_spreads_membership_after_partition_heals(monkeypatch):
    choices = {}

    def choose_next(candidates):
        key = tuple(candidates)
        index = choices.get(key, 0)
        choices[key] = index + 1
        return candidates[index % len(candidates)]

    monkeypatch.setattr("random.choice", choose_next)

    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(10, 10))

    initial_peers = {
        "A": ["B"],
        "B": ["A"],
        "C": ["B"],
    }
    nodes = {}

    for node_id, peers in initial_peers.items():
        node = NodeDynamo(
            node_id=node_id,
            peers=peers,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            gossip_interval_ms=1000,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    # Block communication between C and the other nodes.
    cut = partition({"C"})
    network.add_rule(cut)

    scheduler.run_until(3000)

    assert nodes["A"].known_members == {"A", "B"}
    assert nodes["B"].known_members == {"A", "B"}
    assert nodes["C"].known_members == {"B", "C"}

    # Restore communication and allow periodic gossip to continue.
    network.remove_rule(cut)
    scheduler.run_until(10000)

    for node in nodes.values():
        assert node.known_members == {"A", "B", "C"}

def make_health_check_cluster():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(10, 10))
    node_ids = ["A", "B", "C"]
    nodes = {}

    for node_id in node_ids:
        node = NodeDynamo(
            node_id=node_id,
            peers=node_ids,
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            health_check_interval_ms=1000,
            failure_timeout_ms=3000,
            # Keep gossip out of these tests so pings/pongs
            # are responsible for refreshing last_seen.
            gossip_interval_ms=100000,
        )
        nodes[node_id] = node
        network.register(node_id, node.on_message)

    return scheduler, network, nodes


def test_healthy_nodes_are_not_suspected():
    scheduler, network, nodes = make_health_check_cluster()

    scheduler.run_until(10000)

    for node_id, node in nodes.items():
        assert node.suspected_nodes == set()

        for peer in nodes:
            if peer != node_id:
                assert node.last_seen[peer] > 0


def test_isolated_node_is_suspected_after_timeout():
    scheduler, network, nodes = make_health_check_cluster()
    network.add_rule(partition({"B"}))

    # First health check at 1000 ms starts B's grace period.
    scheduler.run_until(3999)

    assert "B" not in nodes["A"].suspected_nodes
    assert "B" not in nodes["C"].suspected_nodes

    # At 4000 ms, B has been silent for the full 3000 ms.
    scheduler.run_until(4000)

    assert nodes["A"].suspected_nodes == {"B"}
    assert nodes["C"].suspected_nodes == {"B"}

    # From isolated B's perspective, A and C are unreachable.
    assert nodes["B"].suspected_nodes == {"A", "C"}

    for node_id, node in nodes.items():
        assert node_id not in node.suspected_nodes


def test_suspicion_clears_after_partition_heals():
    scheduler, network, nodes = make_health_check_cluster()
    cut = partition({"B"})
    network.add_rule(cut)

    scheduler.run_until(4000)

    assert "B" in nodes["A"].suspected_nodes
    assert "B" in nodes["C"].suspected_nodes

    network.remove_rule(cut)

    # Next probes go out at 5000 ms; replies arrive by 5020 ms.
    scheduler.run_until(5100)

    for node in nodes.values():
        assert node.suspected_nodes == set()

    assert nodes["A"].last_seen["B"] >= 5000
    assert nodes["C"].last_seen["B"] >= 5000

def test_write_replicates_same_clock_to_all_nodes():
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

    request_id = nodes["A"].client_put("cart:42", {"book": 2})

    # The client write increments A's counter immediately.
    assert nodes["A"].clocks["cart:42"] == {"A": 1}

    scheduler.run_until(200)

    assert request_id in nodes["A"].successful_writes

    for node in nodes.values():
        assert node.store["cart:42"] == {"book": 2}
        # Receiving a replica must not increment the receiver's counter.
        assert node.clocks["cart:42"] == {"A": 1}


def test_update_through_another_node_preserves_clock_history():
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

    nodes["A"].client_put("cart:42", {"book": 2})
    scheduler.run_until(200)

    # B must receive A's version before updating it.
    assert nodes["B"].clocks["cart:42"] == {"A": 1}
    previous_clock = nodes["B"].clocks["cart:42"]

    request_id = nodes["B"].client_put("cart:42", {"book": 3})

    assert nodes["B"].clocks["cart:42"] == {"A": 1, "B": 1}
    assert previous_clock == {"A": 1}

    scheduler.run_until(400)

    assert request_id in nodes["B"].successful_writes

    for node in nodes.values():
        assert node.store["cart:42"] == {"book": 3}
        assert node.clocks["cart:42"] == {"A": 1, "B": 1}

def make_single_node():
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0)

    node = NodeDynamo(
        node_id="A",
        peers=["A"],
        transport=network.endpoint("A"),
        scheduler=scheduler,
    )
    network.register("A", node.on_message)
    return node


def test_receive_accepts_new_key():
    node = make_single_node()

    node.receive("cart:42", {"book": 2}, {"B": 1})

    assert node.store["cart:42"] == {"book": 2}
    assert node.clocks["cart:42"] == {"B": 1}
    assert node.log == [("cart:42", {"book": 2})]


def test_receive_applies_newer_version():
    node = make_single_node()
    node.receive("cart:42", {"book": 2}, {"B": 1})

    node.receive("cart:42", {"book": 3}, {"B": 2})

    assert node.store["cart:42"] == {"book": 3}
    assert node.clocks["cart:42"] == {"B": 2}
    assert node.log == [
        ("cart:42", {"book": 2}),
        ("cart:42", {"book": 3}),
    ]


def test_receive_ignores_delayed_older_version():
    node = make_single_node()
    node.receive("cart:42", {"book": 3}, {"B": 2})

    # An older message arrives after the newer version.
    node.receive("cart:42", {"book": 2}, {"B": 1})

    assert node.store["cart:42"] == {"book": 3}
    assert node.clocks["cart:42"] == {"B": 2}
    assert node.log == [("cart:42", {"book": 3})]


def test_receive_ignores_duplicate_version():
    node = make_single_node()
    node.receive("cart:42", {"book": 2}, {"B": 1})

    # Deliver the same version again.
    node.receive("cart:42", {"book": 2}, {"B": 1})

    assert node.store["cart:42"] == {"book": 2}
    assert node.clocks["cart:42"] == {"B": 1}
    assert node.log == [("cart:42", {"book": 2})]

def test_receive_keeps_concurrent_versions_in_either_order():
    key = "cart:42"
    books = ({"book": 3}, {"A": 2})
    books_and_pen = ({"book": 2, "pen": 1}, {"A": 1, "B": 1})

    for first, second in ((books, books_and_pen), (books_and_pen, books)):
        node = make_single_node()
        node.receive(key, *first)
        node.receive(key, *second)

        siblings = node.versions[key]
        assert len(siblings) == 2
        assert books in siblings
        assert books_and_pen in siblings


def test_receive_replaces_only_the_sibling_it_supersedes():
    node = make_single_node()
    key = "cart:42"
    older = ({"book": 3}, {"A": 2})
    concurrent = ({"book": 2, "pen": 1}, {"A": 1, "B": 1})
    newer = ({"book": 4}, {"A": 3})

    node.receive(key, *older)
    node.receive(key, *concurrent)
    node.receive(key, *newer)

    siblings = node.versions[key]
    assert len(siblings) == 2
    assert newer in siblings
    assert concurrent in siblings
    assert older not in siblings


def test_receive_does_not_add_duplicate_when_siblings_exist():
    node = make_single_node()
    key = "cart:42"
    books_and_pen = ({"book": 2, "pen": 1}, {"A": 1, "B": 1})
    books = ({"book": 4}, {"A": 3})

    node.receive(key, *books_and_pen)
    node.receive(key, *books)

    before = node.versions[key].copy()
    node.receive(key, *books)

    assert len(node.versions[key]) == 2
    assert node.versions[key] == before
    
def make_read_pair(read_quorum=2):
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=0, latency_ms=(30, 80))
    nodes = {}

    for node_id in ("A", "B"):
        node = NodeDynamo(
            node_id=node_id,
            peers=["A", "B"],
            transport=network.endpoint(node_id),
            scheduler=scheduler,
            read_quorum=read_quorum,
        )
        network.register(node_id, node.on_message)
        nodes[node_id] = node

    return scheduler, nodes["A"], nodes["B"]


def test_read_keeps_concurrent_versions_from_different_replicas():
    scheduler, a, b = make_read_pair()
    key = "cart:42"
    books = ({"book": 3}, {"A": 2})
    books_and_pen = ({"book": 2, "pen": 1}, {"A": 1, "B": 1})

    a.receive(key, *books)
    b.receive(key, *books_and_pen)

    request_id = a.client_get(key)
    assert request_id in a.pending_reads
    assert request_id not in a.completed_reads

    scheduler.run_until(200)

    result = a.completed_reads[request_id]
    assert len(result) == 2
    assert books in result
    assert books_and_pen in result
    assert request_id not in a.pending_reads

    # Combining read results doesn't change either replica's storage.
    assert a.versions[key] == [books]
    assert b.versions[key] == [books_and_pen]


def test_read_deduplicates_versions_reported_by_both_replicas():
    scheduler, a, b = make_read_pair()
    key = "cart:42"
    version = ({"book": 3}, {"A": 2})

    a.receive(key, *version)
    b.receive(key, *version)

    request_id = a.client_get(key)
    scheduler.run_until(200)

    assert a.completed_reads[request_id] == [version]


def test_read_keeps_newer_version_when_replicas_disagree():
    scheduler, a, b = make_read_pair()
    key = "cart:42"
    older = ({"book": 2}, {"A": 1})
    newer = ({"book": 3}, {"A": 2})

    a.receive(key, *older)
    b.receive(key, *newer)

    request_id = a.client_get(key)
    scheduler.run_until(200)

    assert a.completed_reads[request_id] == [newer]
    assert a.versions[key] == [older]


def test_two_local_versions_still_count_as_one_response():
    scheduler, a, _ = make_read_pair()
    key = "cart:42"
    a.receive(key, {"book": 3}, {"A": 2})
    a.receive(key, {"pen": 1}, {"B": 1})

    request_id = a.client_get(key)

    assert len(a.pending_reads[request_id]) == 1
    assert request_id not in a.completed_reads

    # B has no version, but its empty reply still counts toward quorum.
    scheduler.run_until(200)

    assert a.completed_reads[request_id] == a.versions[key]


def test_read_quorum_one_returns_local_versions_immediately():
    _, a, _ = make_read_pair(read_quorum=1)
    key = "cart:42"
    a.receive(key, {"book": 3}, {"A": 2})
    a.receive(key, {"pen": 1}, {"B": 1})

    request_id = a.client_get(key)

    assert a.completed_reads[request_id] == a.versions[key]
    assert request_id not in a.pending_reads

def test_resolution_write_replaces_both_observed_siblings():
    node = make_single_node()
    key = "cart:42"

    node.receive(key, {"book": 3}, {"A": 2})
    node.receive(key, {"book": 2, "pen": 1}, {"A": 1, "B": 1})

    node.client_put(
        key,
        {"book": 3, "pen": 1},
        context=[{"A": 2}, {"A": 1, "B": 1}],
    )

    assert node.versions[key] == [
        ({"book": 3, "pen": 1}, {"A": 3, "B": 1})
    ]


def test_resolution_write_preserves_unseen_concurrent_edit():
    node = make_single_node()
    key = "cart:42"
    unseen = ({"notebook": 1}, {"C": 1})

    node.receive(key, {"book": 3}, {"A": 2})
    node.receive(key, {"book": 2, "pen": 1}, {"A": 1, "B": 1})
    node.receive(key, *unseen)

    # The client observed A's and B's versions, but not C's.
    node.client_put(
        key,
        {"book": 3, "pen": 1},
        context=[{"A": 2}, {"A": 1, "B": 1}],
    )

    siblings = node.versions[key]
    assert len(siblings) == 2
    assert unseen in siblings
    assert ({"book": 3, "pen": 1}, {"A": 3, "B": 1}) in siblings


def test_write_with_old_context_advances_own_counter():
    node = make_single_node()
    key = "cart:42"
    node.receive(key, {"book": 5}, {"A": 5})

    node.client_put(key, {"book": 6}, context=[{"A": 2}])

    assert node.versions[key] == [
        ({"book": 6}, {"A": 6})
    ]


def test_empty_context_does_not_include_unseen_peer_history():
    node = make_single_node()
    key = "cart:42"
    existing = ({"book": 2}, {"A": 2, "B": 1})
    node.receive(key, *existing)

    node.client_put(key, {"pen": 1}, context=[])

    # A advances its own counter but doesn't copy B's unseen history.
    siblings = node.versions[key]
    assert len(siblings) == 2
    assert existing in siblings
    assert ({"pen": 1}, {"A": 3}) in siblings