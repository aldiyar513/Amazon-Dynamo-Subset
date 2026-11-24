import pytest

from dslabs.hash_ring import select_fallback


@pytest.mark.parametrize(
    ("suspected", "expected"),
    [
        ({"D"}, "E"),                # E is the first eligible fallback.
        ({"D", "E"}, "A"),           # E is suspected, so wrap around to A.
        ({"D", "E", "A"}, None),     # Neither fallback is available.
    ],
)
def test_select_fallback(monkeypatch, suspected, expected):
    ring = [(10, "A"), (20, "B"), (30, "C"), (40, "D"), (50, "E")]
    homes = ["B", "C", "D"]

    # Give the key position 15 so the clockwise order is B, C, D, E, A.
    monkeypatch.setattr("dslabs.hash_ring.hash_position", lambda key: 15)

    assert select_fallback("cart:42", ring, homes, suspected) == expected