from dslabs.vector_clock import compare_clocks


def test_left_clock_is_before_right():
    assert compare_clocks({"A": 1}, {"A": 1, "B": 1}) == "before"


def test_left_clock_is_after_right():
    assert compare_clocks({"A": 1, "B": 1}, {"A": 1}) == "after"


def test_identical_clocks_are_equal():
    assert compare_clocks({"A": 1}, {"A": 1}) == "equal"


def test_independent_updates_are_concurrent():
    # Left is ahead on A; right is ahead on B.
    left = {"A": 2}
    right = {"A": 1, "B": 1}

    assert compare_clocks(left, right) == "concurrent"