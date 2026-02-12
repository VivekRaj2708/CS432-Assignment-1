import asyncio
from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from collections import deque

async def Main(queue, stop_event):
    register = MapRegister()
    updates = deque()
    register.Load("final_map_register.pkl")
    printed = 0
    # Start the streaming task INSIDE the loop
    task = asyncio.create_task(
        stream_sse_records(
            count=1000000, 
            queue=queue, 
            stop_event=stop_event, 
            max_queue_size=10
        )
    )

    try:
        # print first 10 records as a quick smoke test
        while printed < 1000:
            if queue:
                record = queue.popleft()
                # print(record)
                register.ResolveRequest(record, updateOrder=updates)
                printed += 1
            else:
                await asyncio.sleep(0.1)
    finally:
        stop_event.set()
        with open("Map.log", "w") as f:
            f.write(repr(register))
        for update in updates:
            print(update)
        register.Save("final_map_register.pkl")
        await task # Now we safely await the task we created

async def main():
    queue = deque()
    stop_event = asyncio.Event()
    await Main(queue, stop_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
    

    

        
