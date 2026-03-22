import asyncio
from collections import deque
import json

from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from Utils.BiTemporal import attach_bitemporal
from Utils.Classify import FieldClassifier
from sql_logger import sql_from_queue
from mongo_logger import mongo_from_queue
from Storage.MySQLClient import MySQLClient
from Storage.MongoClient import MongoDBClient
from Utils.schema_maker import SchemaInfere
from Utils.sse_parser import parse_sse_queue
from Utils.MySQL.query_executer import MySQLQueryExecutor
from Utils.MongoDB.Exec import Exec


# from dotenv import load_dotenv
# load_dotenv()
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

def _ops_log_to_mongo_queries(log_path: str): #reads and converts the operations.log entrys
    queries = []
    try:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                op = json.loads(line)

                op_type = op.get("type", "").upper()
                entity  = op.get("table_name", "")

                if op_type == "INSERT":
                    data = dict(zip(op.get("columns", []), op.get("values", [])))
                    queries.append({"action": "add", "entity": entity, "data": data})

                elif op_type == "UPDATE":
                    data  = dict(zip(op.get("columns", []), op.get("values", [])))
                    where = op.get("where", {})
                    queries.append({"action": "change", "entity": entity,
                                    "where": where, "data": data})

                elif op_type == "DELETE":
                    queries.append({"action": "remove", "entity": entity,
                                    "where": op.get("where", {})})

                elif op_type == "SELECT":
                    queries.append({"action": "get", "entity": entity,
                                    "fields": op.get("columns", []),
                                    "where": op.get("where", {})})

    except FileNotFoundError:
        pass
    return queries

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

    schema_engine= None        # created after first init event
    schema_raw_buffer= deque()     # collects raw items for schema engine
    schema_initialised= False
    mongo_exec= None
    os.makedirs("schema_output", exist_ok=True)

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
                schema_raw_buffer.append(record)
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
                    #running schema engine
                    try:
                        unique_fields, global_key, event_queue = parse_sse_queue(
                            deque(schema_raw_buffer)
                        )
                        if not schema_initialised:
                            schema_engine = SchemaInfere(
                                unique_fields = unique_fields,
                                global_key    = global_key,
                                output_dir    = "schema_output",
                            )
                            sql_executor = MySQLQueryExecutor(
                                host     = "localhost",
                                user     = "root",
                                password = p,
                                database = "university",
                            )
                            sql_executor.connect()

                            mongo_exec = Exec(db_name="university", worker_count=3)
                            await mongo_exec.start()

                            schema_initialised = True

                        schema_engine.queue_reader(event_queue)
                        sql_executor.execute_generated_queries(
                            "schema_output/operations_sql.json",
                            stop_on_error = False,
                        )
                        sql_executor.save_execution_report(
                            f"schema_output/execution_report_batch_{batch_num}.json"
                        )
                        
                        mongo_queries = _ops_log_to_mongo_queries(
                            "schema_output/operations.log"
                        )
                        await mongo_exec.add_many_to_queue(mongo_queries)

                    except ValueError:
                        # when init event not yet seen
                        pass

                    schema_raw_buffer.clear()

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

        #schema flush for any remaining buffered items
        if schema_raw_buffer and schema_engine:
            try:
                _, _, event_queue = parse_sse_queue(deque(schema_raw_buffer))
                schema_engine.queue_reader(event_queue)

                sql_executor.execute_generated_queries(
                    "schema_output/operations_sql.json",
                    stop_on_error = False,
                )
                sql_executor.save_execution_report(
                    "schema_output/execution_report_final.json"
                )
                
                mongo_queries = _ops_log_to_mongo_queries(
                    "schema_output/operations.log"
                )
                await mongo_exec.add_many_to_queue(mongo_queries)

            except ValueError:
                pass
        
        if schema_initialised:
            sql_executor.disconnect()

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
