def compare_clocks(left: dict[str, int], right: dict[str, int]) -> str:
    left_greater = False
    right_greater = False

    all_keys = set(left.keys()).union(set(right.keys()))
    for key in all_keys:
        left_value = left.get(key, 0)
        right_value = right.get(key, 0)

        if left_value > right_value:
            left_greater = True
        elif left_value < right_value:
            right_greater = True

    if left_greater and not right_greater:
        return "after"
    elif right_greater and not left_greater:
        return "before"
    elif not left_greater and not right_greater:
        return 'equal'
    else:
        return "concurrent"

def increment_clock(clock: dict[str, int], node_id: str) -> dict[str, int]:
    new_clock = clock.copy()
    new_clock[node_id] = new_clock.get(node_id, 0) + 1
    return new_clock

def merge_clocks(clocks: list[dict[str, int]]) -> dict[str, int]:
    merged: dict[str, int] = {}
    # Loop over each clock, then over its node IDs and counters.
    for clock in clocks:
        for node_id, counter in clock.items():
            merged[node_id] = max(merged.get(node_id, 0), counter)
    return merged