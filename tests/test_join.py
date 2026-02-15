import mysql.connector
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()
import os

p = os.getenv("p")


sql_conn = mysql.connector.connect(
    host="localhost",
    user="root",   
    password=p, 
    database="test_output" 
)

sql_cursor = sql_conn.cursor(dictionary=True)

mongo_db = MongoClient("mongodb://localhost:27017")["ingestion"]

sql_cursor.execute("SELECT * FROM root LIMIT 5")
rows = sql_cursor.fetchall()

print(f"{'sys_ingested_at':<25} {'SQL username':<20} {'Mongo found'}")
print("-" * 60)

for row in rows:
    key = row.get("sys_ingested_at")
    mongo_doc = mongo_db["root"].find_one({"sys_ingested_at": key})
    found = "YES" if mongo_doc else "NO -- JOIN BROKEN"
    sql_user = row.get("username", "N/A")
    print(f"{str(key):<25} {sql_user:<20} {found}")

sql_cursor.close()
sql_conn.close()
