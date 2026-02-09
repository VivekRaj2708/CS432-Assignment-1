import requests
import time
import json
import asyncio
from collections import deque
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s')

file_handler = logging.FileHandler('logs.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

session = requests.Session()
session.headers.update({
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache'
})

def fetch_data():
    bust = int(time.time())
    response = requests.get(f"http://127.0.0.1:8000/?_={bust}", headers={'Cache-Control': 'no-cache'})
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


async def stream_sse_records(count, queue: deque, stop_event: asyncio.Event = None, url=None, timeout=None, max_queue_size: int = None):
    if url is None:
        bust = int(time.time())
        url = f"http://127.0.0.1:8000/record/{count}?_={bust}"

    headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache'
    }
    try:
        import httpx
    except Exception:
        logger.error('httpx is required for async SSE streaming')
        raise

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream('GET', url, headers=headers) as resp:
            resp.raise_for_status()
            buffer = []
            async for raw_line in resp.aiter_lines():
                # termination check
                if stop_event is not None and stop_event.is_set():
                    logger.info('Stop event set, ending SSE stream')
                    break

                # pause if queue is too large
                if max_queue_size is not None and len(queue) >= max_queue_size:
                    logger.info('Queue reached max size (%d). Pausing ingestion.', max_queue_size)
                    while (len(queue) >= max_queue_size) and (stop_event is None or not stop_event.is_set()):
                        await asyncio.sleep(0.01)
                    logger.info('Queue below threshold. Resuming ingestion.')

                if raw_line is None:
                    continue
                line = raw_line.strip()
                # blank line -> dispatch event
                if not line:
                    if buffer:
                        data_lines = [l[len('data: '):].lstrip() if l.startswith('data:') else l for l in buffer]
                        data = '\n'.join(data_lines)
                        try:
                            queue.append(json.loads(data))
                        except Exception:
                            queue.append(data)
                        buffer = []
                    continue
                buffer.append(line)

async def main():
    queue = deque()
    stop_event = asyncio.Event()
    # start streaming in background
    task = asyncio.create_task(stream_sse_records(1000000, queue, stop_event, max_queue_size=10))
    printed = 0
    try:
        # print first 10 records as a quick smoke test
        while True:
            if queue:
                record = queue.popleft()
                print(record)
                printed += 1
            else:
                await asyncio.sleep(1)
    finally:
        stop_event.set()
        await task

try:
    asyncio.run(main())
except Exception as e:
    print(f"Error: {e}")