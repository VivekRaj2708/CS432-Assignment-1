# Network Module Documentation

## Overview
The `Network` module provides utilities for fetching data from HTTP endpoints and streaming Server-Sent Events (SSE) asynchronously. It includes cache-busting mechanisms and automatic reconnection logic for robust real-time data ingestion.

## Functions

### `fetch_data()`
Synchronously fetches JSON data from the default endpoint with cache-busting.

**Returns:**
- `dict`: Parsed JSON response from the server

**Raises:**
- `requests.HTTPError`: If the response status code is not 200

**Example:**
```python
from Utils.Network import fetch_data

try:
    data = fetch_data()
    print(f"Received: {data}")
except requests.HTTPError as e:
    print(f"Failed to fetch data: {e}")
```

**Implementation Details:**
- Uses a cache-busting query parameter (`_=<timestamp>`) to prevent browser/proxy caching
- Sends `Cache-Control: no-cache` headers
- Default endpoint: `http://127.0.0.1:8000/`

---

### `stream_sse_records(count, queue, stop_event=None, url=None, timeout=None, max_queue_size=None, reconnect_delay=0.1)`
Asynchronously streams Server-Sent Events (SSE) from an endpoint and pushes parsed JSON records into a queue. Automatically reconnects on connection loss unless stopped.

**Parameters:**
- `count` (int): Record count parameter passed to the endpoint (used in default URL)
- `queue` (deque): A `collections.deque` instance where parsed records will be appended
- `stop_event` (asyncio.Event, optional): Event to signal graceful shutdown. When set, the stream stops and the function returns
- `url` (str, optional): Custom SSE endpoint URL. Defaults to `http://127.0.0.1:8000/record/{count}`
- `timeout` (float, optional): HTTP client timeout in seconds. Defaults to `httpx` library default
- `max_queue_size` (int, optional): Maximum queue size. When reached, ingestion pauses until queue drops below threshold
- `reconnect_delay` (float, optional): Seconds to wait before reconnecting after disconnect. Default: 0.1

**Returns:**
- None (runs until `stop_event` is set)

**Raises:**
- `ImportError`: If `httpx` is not installed
- Other exceptions are logged and trigger reconnection

**Behavior:**
1. Connects to the SSE endpoint with cache-busting
2. Parses lines starting with `data:` as JSON
3. Adds a `t_stamp` field (current Unix timestamp) to each record
4. Appends records to the queue
5. On disconnect or error, waits `reconnect_delay` seconds and reconnects
6. Stops when `stop_event` is set or on unrecoverable errors

**Example:**
```python
import asyncio
from collections import deque
from Utils.Network import stream_sse_records

async def main():
    queue = deque()
    stop_event = asyncio.Event()
    
    # Start streaming in background
    task = asyncio.create_task(
        stream_sse_records(
            count=100,
            queue=queue,
            stop_event=stop_event,
            max_queue_size=1000
        )
    )
    
    # Process records
    try:
        while True:
            if queue:
                record = queue.popleft()
                print(f"Received: {record}")
            else:
                await asyncio.sleep(0.1)
    finally:
        # Graceful shutdown
        stop_event.set()
        await task

asyncio.run(main())
```

**Queue Management:**
- When `max_queue_size` is set, ingestion pauses if `len(queue) >= max_queue_size`
- Ingestion resumes automatically when queue size drops below threshold
- Prevents memory overflow during slow processing

**Auto-Reconnection:**
- Reconnects automatically on network errors or stream interruptions
- Respects `stop_event` even during reconnection delays
- Uses exponential backoff via `reconnect_delay` parameter

---

## Dependencies

- **requests**: For synchronous HTTP requests (`fetch_data`)
- **httpx**: For asynchronous HTTP streaming (`stream_sse_records`)
- **asyncio**: For async task management
- **collections.deque**: For efficient queue operations

Install with:
```bash
pip install requests httpx
```

---

## Configuration

### Default Endpoints
- **Fetch**: `http://127.0.0.1:8000/`
- **Stream**: `http://127.0.0.1:8000/record/{count}`

### Cache-Busting
Both functions append a unique timestamp query parameter (`_=<unix_timestamp>`) to prevent caching by browsers, proxies, or CDNs.

### Headers
- `Cache-Control: no-cache, no-store, must-revalidate`
- `Pragma: no-cache`
- `Accept: text/event-stream` (for SSE streaming)

---

## Error Handling

### `fetch_data()`
- Raises `HTTPError` for non-200 status codes
- Network errors propagate to caller

### `stream_sse_records()`
- Logs exceptions via `Utils.Log.logger`
- Automatically reconnects on:
  - Network errors
  - Connection drops
  - JSON parsing errors (record skipped, stream continues)
- Stops gracefully when `stop_event` is set

---

## Best Practices

1. **Always use `stop_event`** for graceful shutdown in production
2. **Set `max_queue_size`** to prevent memory exhaustion during processing slowdowns
3. **Tune `reconnect_delay`** based on your server's rate limits (increase for aggressive rate limiting)
4. **Process queue in separate task** to avoid blocking ingestion
5. **Monitor queue size** to detect processing bottlenecks

**Example: Separate Processing Task**
```python
async def process_records(queue, stop_event):
    while not stop_event.is_set() or queue:
        if queue:
            record = queue.popleft()
            # Process record...
        else:
            await asyncio.sleep(0.01)

async def main():
    queue = deque()
    stop_event = asyncio.Event()
    
    stream_task = asyncio.create_task(stream_sse_records(10, queue, stop_event))
    process_task = asyncio.create_task(process_records(queue, stop_event))
    
    # Run for some time...
    await asyncio.sleep(60)
    
    stop_event.set()
    await asyncio.gather(stream_task, process_task)
```

---

## Testing

The module includes a built-in test:
```bash
python Utils/Network.py
```

Or, the system has in built testing suites for CI/CD. To run the testing suite make sure that you have `pytest` and `pytest-async` installed. To do so run:

```bash
pip install pytest pytest-async
```

After installation, to initialise the tests use:
```bash
pytest -q
```

This runs the `main()` function which streams records and prints them to console.

