import asyncio
from collections import deque

from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from Utils.BiTemporal import attach_bitemporal
from Utils.Classify import FieldClassifier
from sql_logger import sql_from_queue
from mongo_logger import mongo_from_queue
from Storage.MySQLClient import MySQLClient
from Storage.MongoClient import MongoDBClient


from dotenv import load_dotenv
load_dotenv()
import os

p = os.getenv("p")



BATCH_SIZE = 1000
TOTAL      = 10000


async def _flush(
    updates,
    classifier: FieldClassifier,
    mysql_client: MySQLClient,
    mongo_client: MongoDBClient,
    batch_num,
):

    sql_file   = f"sql_queries_batch_{batch_num}.log"
    mongo_file = f"mongo_queries_batch_{batch_num}.log"

    sql_from_queue(updates,   filename=sql_file,   classifier=classifier)
    mongo_from_queue(updates, filename=mongo_file, classifier=classifier)

    sql_result = mysql_client.execute_log_file(sql_file)
    mongo_result = mongo_client.execute_log_file(mongo_file)

    print(
        f"[Batch {batch_num}] flushed — "
        f"MySQL: {sql_result} | "
        f"MongoDB: {mongo_result}"
    )


async def Main(queue, stop_event):
    register   = MapRegister()
    updates    = deque()
    classifier = FieldClassifier()

    register.Load("final_map_register.pkl")

    mysql_client = MySQLClient(
        host     = "localhost",
        port     = 3306,
        user     = "root",
        password = p,
        database = "test_output",
    )
    mongo_client = MongoDBClient(
        uri     = "mongodb://localhost:27017",
        db_name = "ingestion",
    )

    printed     = 0
    batch_count = 0
    batch_num   = 1

    task = asyncio.create_task(
        stream_sse_records(
            count         = 10000,
            queue         = queue,
            stop_event    = stop_event,
            max_queue_size= 10
        )
    )

    try:
        while printed < TOTAL:
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
                printed     += 1
                batch_count += 1

                if batch_count >= BATCH_SIZE:
                    await _flush(updates, classifier, mysql_client, mongo_client, batch_num)
                    updates.clear()
                    batch_count = 0
                    batch_num  += 1

            else:
                await asyncio.sleep(0.1)

    finally:
        stop_event.set()

        if updates:
            await _flush(updates, classifier, mysql_client, mongo_client, batch_num="final")

        with open("Map.log", "w") as f:
            f.write(repr(register))

        mysql_client.disconnect()
        mongo_client.disconnect()

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