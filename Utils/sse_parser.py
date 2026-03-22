import json
from collections import deque
_KNOWN_EVENTS = {"init", "create", "add", "change", "remove", "get"}


def parse_sse_queue(raw_queue):
    unique_fields = []
    global_key    = None
    event_queue   = deque()

    for item in raw_queue:
        event, record = _parse_item(item)

        if event == "init":
            # Extract engine config from init payload
            global_key, unique_fields = _extract_init_config(record)
            continue   # init is never forwarded as an operation

        if event not in _KNOWN_EVENTS:
            # Unknown event type — skip silently
            continue

        event_queue.append((event, record))

    if global_key is None:
        raise ValueError(
            "No 'init' event found in the queue. "
            "SchemaInfere needs an init event to know unique_fields and global_key."
        )

    return unique_fields, global_key, event_queue

def _parse_item(item):
    # Already a tuple — trust it
    if isinstance(item, tuple) and len(item) == 2:
        event, record = item
        if isinstance(record, str):
            record = json.loads(record)
        return event.lower().strip(), record

    # String — could be a JSON-encoded envelope or raw SSE
    if isinstance(item, str):
        stripped = item.strip()
        if stripped.startswith("{"):
            try:
                item = json.loads(stripped)   # parse then fall through to dict handling
            except json.JSONDecodeError:
                pass   # not valid JSON, treat as raw SSE string
        if isinstance(item, str):
            return _parse_sse_string(item)

    # Dict — check for the {"event": ..., "data": {...}} envelope format
    if isinstance(item, dict):
        if "event" in item and "data" in item:
            # Primary format shown in the images
            event  = str(item["event"]).lower().strip()
            record = item["data"]
            if isinstance(record, str):
                record = json.loads(record)
            return event, record
        else:
            # Plain record dict with no envelope — treat as create
            return "create", item

    # String — parse SSE lines
    if isinstance(item, str):
        return _parse_sse_string(item)

    raise TypeError(f"Unsupported queue item type: {type(item)}")


def _parse_sse_string(text):
    event  = "create"   # default if no event: line present
    record = None

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("event:"):
            event = line.split(":", 1)[1].strip().lower()
        elif line.startswith("data:"):
            raw_json = line.split(":", 1)[1].strip()
            record = json.loads(raw_json)

    if record is None:
        # Try parsing the whole string as bare JSON (no SSE prefix)
        try:
            record = json.loads(text.strip())
        except json.JSONDecodeError:
            raise ValueError(f"Could not parse SSE item — no data: line found:\n{text!r}")

    return event, record


def _extract_init_config(config):
    global_key    = None
    unique_fields = []

    for field, meta in config.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("global_key") == "true":
            global_key = field
        if meta.get("unique") == "true" and meta.get("global_key") != "true":
            unique_fields.append(field)

    return global_key, unique_fields




if __name__ == "__main__":
    sample_queue = deque([
        # Format 1: multi-line SSE string
        'event: init\ndata: {"username": {"global_key": "true", "unique": "true"}, "student_id": {"unique": "true"}, "course_id": {"unique": "true"}, "dept_name": {"unique": "true"}, "title": {}, "credits": {}}',

        # Format 2: multi-line SSE string (create)
        'event: create\ndata: {"student_id": "S1", "dept_name": "CSE", "username": "alice"}',

        # Format 3: bare data: line — event defaults to create
        'data: {"course_id": "CS101", "title": "Intro to CS", "credits": 3, "username": "bob"}',

        # Format 4: plain dict — treated as create
        {"student_id": "S2", "dept_name": "EE", "username": "carol"},

        # Format 5: already-split tuple
        ("get", {"student_id": "S1", "username": "alice", "COLUMNS": ["dept_name"]}),

        # Format 6: change event
        ("change", {"course_id": "CS101", "credits": 4, "username": "admin"}),

        # Format 7: remove event
        ("remove", {"student_id": "S2", "username": "admin"}),
    ])

    unique_fields, global_key, event_queue = parse_sse_queue(sample_queue)

    print(f"global_key    : {global_key}")
    print(f"unique_fields : {unique_fields}")
    print(f"event_queue   : {len(event_queue)} items")
    print()
    for ev, rec in event_queue:
        print(f"  ({ev!r:8})  {rec}")