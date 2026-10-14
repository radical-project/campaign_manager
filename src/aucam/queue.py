"""WorkflowQueue: priority queue of LaunchRequests with aging."""

import heapq
import time

from .types import LaunchPriority, LaunchRequest, QueueSnapshot

_PRI = {
    LaunchPriority.CRITICAL: 0,
    LaunchPriority.HIGH: 1,
    LaunchPriority.NORMAL: 2,
    LaunchPriority.LOW: 3,
}


class WorkflowQueue:
    """
    Min-heap ordered by (priority_int, enqueued_at, seq).

    A monotonic sequence number prevents Python from comparing LaunchRequest
    objects when priority and enqueued_at are equal.
    NORMAL requests age to HIGH after `age_threshold_s` seconds waiting.
    """

    def __init__(self, age_threshold_s: float = 300.0) -> None:
        self._heap: list[tuple] = []
        self._age_threshold_s = age_threshold_s
        self._seq = 0

    def push(self, request: LaunchRequest) -> None:
        pri = _PRI[request.priority]
        heapq.heappush(self._heap, (pri, request.enqueued_at, self._seq, request))
        self._seq += 1

    def pop(self) -> LaunchRequest | None:
        self._apply_aging()
        if not self._heap:
            return None
        _, _, _, req = heapq.heappop(self._heap)
        return req

    def peek(self) -> LaunchRequest | None:
        self._apply_aging()
        if not self._heap:
            return None
        return self._heap[0][3]

    def remove_by_type(self, workflow_type: str, count: int) -> int:
        """Evict up to `count` lowest-priority requests of `workflow_type`."""
        targets = sorted(
            [(pri, t, seq, req) for pri, t, seq, req in self._heap if req.workflow_type == workflow_type],
            key=lambda e: (-e[0], e[1]),
        )
        to_remove = {id(e[3]) for e in targets[:count]}
        removed = 0
        new_heap = []
        for entry in self._heap:
            if id(entry[3]) in to_remove:
                removed += 1
            else:
                new_heap.append(entry)
        heapq.heapify(new_heap)
        self._heap = new_heap
        return removed

    def snapshot(self) -> QueueSnapshot:
        by_pri: dict[str, int] = {}
        by_type: dict[str, int] = {}
        now = time.time()
        oldest = 0.0
        for _, enqueued_at, _, req in self._heap:
            by_pri[req.priority.value] = by_pri.get(req.priority.value, 0) + 1
            by_type[req.workflow_type] = by_type.get(req.workflow_type, 0) + 1
            age = now - enqueued_at
            if age > oldest:
                oldest = age
        return QueueSnapshot(
            total_queued=len(self._heap),
            by_priority=by_pri,
            by_type=by_type,
            oldest_wait_s=oldest,
        )

    def __len__(self) -> int:
        return len(self._heap)

    def candidates(self) -> list[LaunchRequest]:
        """Return all queued requests in priority order without modifying the queue."""
        self._apply_aging()
        return [req for _, _, _, req in sorted(self._heap)]

    def remove_request(self, request: LaunchRequest) -> bool:
        """Remove a specific request by object identity.  Returns True if found."""
        target = id(request)
        new_heap = [(p, t, s, r) for p, t, s, r in self._heap if id(r) != target]
        if len(new_heap) == len(self._heap):
            return False
        heapq.heapify(new_heap)
        self._heap = new_heap
        return True

    def clear(self) -> int:
        """Discard all queued requests. Returns the number removed."""
        n = len(self._heap)
        self._heap = []
        return n

    def __bool__(self) -> bool:
        return bool(self._heap)

    def _apply_aging(self) -> None:
        now = time.time()
        normal_pri = _PRI[LaunchPriority.NORMAL]
        high_pri = _PRI[LaunchPriority.HIGH]
        changed = False
        new_heap = []
        for pri, enqueued_at, seq, req in self._heap:
            if pri == normal_pri and now - enqueued_at > self._age_threshold_s:
                pri = high_pri
                changed = True
            new_heap.append((pri, enqueued_at, seq, req))
        if changed:
            heapq.heapify(new_heap)
            self._heap = new_heap
