import json, statistics
from pathlib import Path

jsonl = Path("aucam-tel-22712136/asyncflow.session.a0197145.1791328913.telemetry.jsonl")
target = "dummy-campaign.sim_worker.fe9a7792"

events = [json.loads(l) for l in jsonl.read_text().splitlines() if l.strip()]

# show first event to confirm field names
print("=== sample event ===")
print(json.dumps(events[0], indent=2))
print()

# find time window using task_id (not wf_id)
t_start = t_end = None
for e in events:
    if e.get("task_id") == target:
        etype = e.get("event_type")
        if etype == "TaskStarted":
            t_start = e.get("event_time")
        elif etype in ("TaskCompleted", "TaskFailed", "TaskCancelled"):
            t_end = e.get("event_time")

print(f"window: {t_start} → {t_end}")

if t_start is None or t_end is None:
    etypes = set(e.get("event_type") for e in events)
    print(f"event_types in file: {etypes}")
    sample_ids = [e.get("task_id") for e in events if e.get("task_id")][:5]
    print(f"sample task_ids: {sample_ids}")
else:
    # cpu_percent may be top-level or inside attributes
    def get_cpu(e):
        v = e.get("cpu_percent")
        if v is None:
            v = (e.get("attributes") or {}).get("cpu_percent")
        return v

    ru_events = [e for e in events
                 if e.get("event_type") == "ResourceUpdate"
                 and t_start <= e.get("event_time", 0) <= t_end]
    cpus = [get_cpu(e) for e in ru_events if get_cpu(e) is not None]

    print(f"ResourceUpdate events in window: {len(ru_events)}, with cpu_percent: {len(cpus)}")
    if cpus:
        print(f"n={len(cpus)}, avg={statistics.mean(cpus):.5f}")
        print(f"expected: n=83, avg=53.09518")
    else:
        # show a sample ResourceUpdate to find where cpu lives
        sample_ru = next((e for e in events if e.get("event_type") == "ResourceUpdate"), None)
        if sample_ru:
            print("sample ResourceUpdate:")
            print(json.dumps(sample_ru, indent=2))
