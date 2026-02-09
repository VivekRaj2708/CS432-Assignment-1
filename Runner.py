import asyncio
from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from collections import deque

if __name__ == "__main__":

    data = deque()
    stop_event = asyncio.Event()
    task = asyncio.create_task(stream_sse_records(100, data, stop_event))

        
