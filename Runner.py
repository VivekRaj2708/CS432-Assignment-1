import asyncio
from collections import deque

from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from Utils.BiTemporal import attach_bitemporal
from Utils.Classify import FieldClassifier
from sql_logger import sql_from_queue
from mongo_logger import mongo_from_queue


async def Main(queue, stop_event):
    register   = MapRegister()
    updates    = deque()
    classifier = FieldClassifier()

    register.Load("final_map_register.pkl")

    printed = 0

    task = asyncio.create_task(
        stream_sse_records(
            count=10000,
            queue=queue,
            stop_event=stop_event,
            max_queue_size=10
        )
    )

    try:
        while printed < 1000:
            if queue:
                record = queue.popleft()

                attach_bitemporal(record)
                sys_ingested_at = record["sys_ingested_at"]
                t_stamp         = record["t_stamp"]

                record_updates = deque()
                register.ResolveRequest(record, updateOrder=record_updates)

                classifier.ingest_alter_events(record_updates)
                classifier.classify_record(record)

                for op in record_updates:
                    if op.get("type") == "INSERT":
                        op["sys_ingested_at"]  = sys_ingested_at
                        op["t_stamp"]          = t_stamp
                        op["_original_record"] = record

                updates.extend(record_updates)
                printed += 1

            else:
                await asyncio.sleep(0.1)

    finally:
        stop_event.set()

        with open("Map.log", "w") as f:
            f.write(repr(register))

        for update in updates:
            print(update)

        sql_from_queue(updates, filename="sql_queries.log", classifier=classifier)
        mongo_from_queue(updates, filename="mongo_queries.log", classifier=classifier)

        register.Save("final_map_register.pkl")
        classifier.save()  

        await task


async def main():
    queue      = deque()
    stop_event = asyncio.Event()
    await Main(queue, stop_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
