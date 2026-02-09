import asyncio
from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from collections import deque

if __name__ == "__main__":

    queue = deque() # ->>>>> Use this queue to do the required computation
    stop_event = asyncio.Event() # ->>>>> Use this to stop further computation

    # Start streaming in different thread
    task = asyncio.create_task(
                stream_sse_records(
                    count=1000000, 
                    queue=queue, 
                    stop_event=stop_event, 
                    max_queue_size=10)
                )
    

    

        
