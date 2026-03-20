import asyncio
from collections import deque

from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from Utils.BiTemporal import attach_bitemporal
from Utils.Classify import FieldClassifier
from sql_logger import sql_schema_maker,sql_from_queue
from mongo_logger import mongo_schema_maker,mongo_from_queue
import glob
import re

def get_latest_register_path():
    files = glob.glob("final_map_register_batch_*.pkl")
    
    if not files:
        return None
    
    file_data = []
    for f in files:
        match = re.search(r"final_map_register_batch_(\d+)\.pkl", f)
        if match:
            # Store as (integer_value, filename)
            file_data.append((int(match.group(1)), f))
    
    if not file_data:
        return None
        
    file_data.sort()
    return file_data[-1][1]


async def Main(queue, stop_event):
    register   = MapRegister()
    updates    = deque()
    classifier = FieldClassifier()

    register.Load(get_latest_register_path())

    records_in_batch = 0
    batch_number = 1
    BATCH_SIZE = 1000

    task = asyncio.create_task( # pushes incoming events to queue
        stream_sse_records(
            queue=queue,
            stop_event=stop_event,
            max_queue_size=1000,
            count=200 ###########1000000
        )
    )

    def flush_batch():
        if not updates:
            return

        sql_file = f"sql_queries_batch_{batch_number}.log"
        mongo_file = f"mongo_queries_batch_{batch_number}.log"
        map_file = f"final_map_register_batch_{batch_number}.pkl"

        sql_from_queue(updates, filename=sql_file, classifier=classifier)
        mongo_from_queue(updates, filename=mongo_file, classifier=classifier)
        register.Save(map_file)
        classifier.save()

        print(f"Batch {batch_number} processed and saved ({records_in_batch} records).")
        
        updates.clear()

    try:
        while True: # now reading queue and processing
            if queue:
                # record = queue.popleft() editing this, event can be record or schema, previously was just record
                ##########
                event = queue.popleft()
                etype = event["event"]
                data  = event["data"]

                if etype=="schema":
                    schema = data
                    sql_schema_maker(schema, filename="sql_schema.log")
                    mongo_schema_maker(schema, filename="mongo_schema.log")
                    continue
                if etype == "record":
                    record = data
                ##########
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
                records_in_batch += 1

                if records_in_batch >= BATCH_SIZE:
                    flush_batch()
                    batch_number += 1
                    records_in_batch = 0

            else:
                await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        pass

    finally:
        stop_event.set()

        if records_in_batch > 0:
            print(f"\nFlushing final incomplete batch {batch_number}...")
            flush_batch()

        with open("Map.log", "w") as f:
            f.write(repr(register))
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass


async def main():
    queue      = deque()
    stop_event = asyncio.Event()

    await Main(queue, stop_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Shutting down safely...")
