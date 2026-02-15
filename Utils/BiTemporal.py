from __future__ import annotations
import time
import threading

_lock = threading.Lock()
_last_ts: float = 0.0


def _unique_server_timestamp() -> float:
    global _last_ts
    with _lock:
        ts = time.time()
        if ts <= _last_ts:
            ts = _last_ts + 1e-6
        _last_ts = ts
    return ts


def attach_bitemporal(record: dict) -> dict:
    if "t_stamp" not in record or record["t_stamp"] is None:
        record["t_stamp"] = time.time()
    record["sys_ingested_at"] = _unique_server_timestamp()
    return record
