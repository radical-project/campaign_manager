"""Shared utility functions used across policy submodules."""

from __future__ import annotations


def _stage_depth(stages: dict) -> dict[str, int]:
    """Dependency-chain depth per stage (roots = 0). Downstream = larger depth."""
    depth: dict[str, int] = {}

    def _d(name: str, seen: frozenset) -> int:
        if name in depth:
            return depth[name]
        deps = stages.get(name, {}).get("deps", [])
        deps = [d for d in deps if d in stages and d not in seen]
        val = 0 if not deps else 1 + max(_d(d, seen | {name}) for d in deps)
        depth[name] = val
        return val

    for s in stages:
        _d(s, frozenset())
    return depth


def _batch_for_bp(bp_state: str, current: int, lo: int = 10, hi: int = 200) -> int:
    """Shrink under THROTTLE, grow under WIDEN, hold otherwise."""
    if bp_state == "THROTTLE":
        return max(lo, current // 2)
    if bp_state == "WIDEN":
        return min(hi, current * 2)
    return current
