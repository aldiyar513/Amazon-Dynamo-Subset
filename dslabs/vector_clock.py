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