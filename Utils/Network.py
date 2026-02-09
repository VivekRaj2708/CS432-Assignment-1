import requests
import time
import json
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



def stream_sse_records(count, queue: deque, url=None, timeout=None):
    """Connect to a Server-Sent Events (SSE) endpoint and yield parsed JSON payloads.

    This parses lines like `data: {...}` and yields the JSON-decoded object for
    each completed event. If JSON decoding fails the raw data string is yielded.
    """
    if url is None:
        bust = int(time.time())
        url = f"http://127.0.0.1:8000/record/{count}?_={bust}"

    headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache'
    }

    with requests.get(url, headers=headers, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if line.startswith("data:"):
                data_str = line[5:].strip()
                queue.append(json.loads(data_str))
            else:
                logger.info('Rejected line: %s', line)


if __name__ == "__main__":
    queue = deque()
    try:
        # Quick smoke: stream 10 records and print them
        stream_sse_records(10, queue)
        while queue:
            record = queue.popleft()
            print(record)
    except Exception as e:
        print(f"Error: {e}")