"""
simulation of user behaviour
sends schema to server
then streams data

user will specify all details in schema using json
the dictionary will have table_name and then dictionaries defining those columns
a column dictionary will have key value pairs as column_name:[column data type, contraint1, constraint2]
the table constraints will also be in the same dictionary, as "table@constraints":[constraint1, constraint2]
the chosen schema is the class university example

the schema is stored in uni_schema.json, imported from there

telemetre is an elec instrument which measures and sends recorded data continuously to some distant station

based on which tables have foreign keys, and which tables they refer to, the order of record creation should be
timeslot
classroom
department
student
instructor
course
section
teaches
takes
advisor
prereq

for every record you add, the logic should be
choose only from valid foreign refs
"""

from fastapi import FastAPI
from faker import Faker
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timedelta
import random
import asyncio
import json
from pathlib import Path

schema_path = Path(__file__).parent / "uni_schema.json" # getting uni_schema.json from parent dir
with open(schema_path) as f:
    uni_schema = json.load(f)

app = FastAPI() # app
faker = Faker() # faker generator

DEPT_POOL = []
COURSE_POOL = []
CLASSROOM_POOL = []
TIMESLOT_POOL = []
SECTION_POOL = []
STUDENT_POOL = []
INSTRUCTOR_POOL = []

init_tables = False

def trim_name(name, max_len=20):
    if len(name) <= max_len:
        return name
    return name[:max_len].rsplit(" ", 1)[0]

names = [trim_name(faker.unique.name()) for _ in range(1100)]
STUDENT_NAMES = names[:1000] # maintaining student and instructor pools to prevent overlap
S_NAME_ID = {} # create ID if not yet else use the one prev created since ID won't change
INSTRUCTOR_NAMES = names[1000:]
I_NAME_ID = {}

DEPT_NAMES = ["Arts", "Maths", "Physics", "Biology", "History", "Geography"] # varchar 20
COURSE_TITLES = [faker.sentence(nb_words=3)[:50] for _ in range(100)]

suffixes = ["Hall", "Building", "Center", "Complex", "Tower"]
BUILDINGS = [(faker.street_name().split()[0]+" "+random.choice(suffixes)) for _ in range(20)]

def gen_timeslot():
    ts_id = faker.bothify(text="T###") # bothify gens strs, match pattern, here # replaced with 0-9, eg T320
    day = random.choice(["Monday","Tuesday","Wednesday","Thursday","Friday"])
    start_hour = random.randint(8, 16)
    start_time = f"{start_hour}:00"
    end_time = f"{start_hour}:50"
    record = {
        "table": "time_slot",
        "time_slot_id": ts_id,
        "day": day,
        "start_time": start_time,
        "end_time": end_time
    }
    TIMESLOT_POOL.append((ts_id, day, start_time)) # will need this as many tables refer to this
    return record

def gen_classroom():
    building = random.choice(BUILDINGS)
    room = faker.bothify(text="R###")[:7]
    record = {
        "table": "classroom",
        "building": building,
        "room_no": room,
        "capacity": random.randint(20, 200)
    }
    CLASSROOM_POOL.append((building, room))
    return record

def gen_dept(dept = None): # reqs CLASSROOM_POOL
    if not dept:
        dept = random.choice(DEPT_NAMES)
    DEPT_POOL.append(dept)
    return {
        "table": "department",
        "dept_name": dept,
        "building": random.choice(BUILDINGS), # varchar 30
        "budget": round(random.uniform(500000, 5000000), 2) # max digs 12, max left to dec pt 2
    }

def gen_student():
    name = random.choice(STUDENT_NAMES)
    if name not in S_NAME_ID:
        S_NAME_ID[name]=faker.bothify(text="S####")
    record = {
        "table": "student",
        "ID": S_NAME_ID[name], # varchar 5
        "name": name, # varchar 20
        "dept_name": random.choice(DEPT_POOL), # varchar 20
        "tot_cred": random.randint(16, 32) # int
    }
    STUDENT_POOL.append(S_NAME_ID[name])
    return record

def gen_instructor():
    name = random.choice(INSTRUCTOR_NAMES) # same logic as student
    if name not in I_NAME_ID.keys():
        I_NAME_ID[name]=faker.bothify(text="I####")
    record = {
        "table": "instructor",
        "ID": I_NAME_ID[name], # varchar 5
        "name": name, # varchar 20
        "dept_name": random.choice(DEPT_POOL),
        "salary": round(random.uniform(100000, 1000000), 2) # max 10 digs total, max 2 after dec pt
    }
    INSTRUCTOR_POOL.append(I_NAME_ID[name])
    return record

def gen_course():
    course_id = faker.bothify(text="CS####")
    record = {
        "table": "course",
        "course_id": course_id,
        "title": random.choice(COURSE_TITLES),
        "dept_name": random.choice(DEPT_POOL),
        "credits": random.randint(1, 5)
    }
    COURSE_POOL.append(course_id)
    return record

def gen_section(): # reqs COURSE_POOL, CLASSROOM_POOL, TIMESLOT_POOL
    course = random.choice(COURSE_POOL) # fk
    building, room = random.choice(CLASSROOM_POOL) # fk
    ts_id, _, _ = random.choice(TIMESLOT_POOL) # fk
    sec_id = faker.bothify(text="SEC##") # varchar 8
    semester = random.choice(["Fall","Spring","Summer","Winter"]) # varchar 6
    year = random.randint(2021, 2025)
    record = {
        "table": "section",
        "course_id": course,
        "sec_id": sec_id,
        "semester": semester,
        "year": year,
        "building": building,
        "room_no": room,
        "time_slot_id": ts_id
    }
    SECTION_POOL.append((course, sec_id, semester, year))
    return record

def gen_teaches(): # reqs INSTRUCTOR_POOL, SECTION_POOL
    instructor = random.choice(INSTRUCTOR_POOL)
    course, sec, sem, year = random.choice(SECTION_POOL)
    return {
        "table": "teaches",
        "ID": instructor,
        "course_id": course,
        "sec_id": sec,
        "semester": sem,
        "year": year
    }

def gen_takes(): # reqs STUDENT_POOL, SECTION_POOL
    student = random.choice(STUDENT_POOL)
    course, sec, sem, year = random.choice(SECTION_POOL)
    return {
        "table": "takes",
        "ID": student,
        "course_id": course,
        "sec_id": sec,
        "semester": sem,
        "year": year,
        "grade": random.choice(["A","B","C","D","F","A-","B+"])
    }

def gen_advisor(): # reqs STUDENT_POOL, INSTRUCTOR_POOL
    return {
        "table": "advisor",
        "s_id": random.choice(STUDENT_POOL),
        "i_id": random.choice(INSTRUCTOR_POOL)
    }

def gen_prereq(): # reqs COURSE_POOL
    if len(COURSE_POOL) < 2: # if less than 2 courses, can't have prereqs
        gen_course()
        gen_course()
    course = random.choice(COURSE_POOL)
    prereq = random.choice(COURSE_POOL)
    while prereq == course: # they shouldn't be the same
        prereq = random.choice(COURSE_POOL)
    return {
        "table": "prereq",
        "course_id": course,
        "prereq_id": prereq
    }

TABLE_GENERATORS = {
    "student": gen_student,
    "instructor": gen_instructor,
    "department": gen_dept,
    "course": gen_course,
    "section": gen_section,
    "classroom": gen_classroom,
    "time_slot": gen_timeslot,
    "takes": gen_takes,
    "teaches": gen_teaches,
    "advisor": gen_advisor,
    "prereq": gen_prereq
}

"""
need to add some bias and randomness to mimic real user input
added randomisation of which table the generated record is of
for sparseness, could randomly make some fields null after creation of record
for metadata, could add details of client like they did
for valid input, first need to add some initial records to ensure foreign key matches
"""

def init_tables(): # for valid input, need to add some initial records to ensure foreign key matches
    global initiated_tabs
    if initiated_tabs:
        return
    for _ in range(10):
        gen_timeslot()
    for _ in range(10):
        gen_classroom()
    for dept in DEPT_NAMES: # generating depts, that's sensible
        gen_dept(dept)
    for _ in range(10):
        gen_student()
        gen_instructor()
    for _ in range(10):
        gen_course()
    for _ in range(10):
        gen_section()
    initiated_tabs = True

TABLE_WEIGHTS = {
    "department": 0.2,
    "student": 0.8,
    "instructor": 0.7,
    "course": 0.6,
    "classroom": 0.3,
    "time_slot": 0.3,
    "section": 0.2,
    "takes": 0.4,
    "advisor": 0.2,
    "prereq": 0.2,
    "teaches": 0.4
}

CAN_BE_NULL = {
    "department": [],
    "student": ["tot_cred"],
    "instructor": ["salary"],
    "course": [],
    "classroom": [],
    "time_slot": [],
    "section": [],
    "takes": [],
    "advisor": [],
    "prereq": [],
    "teaches": []
}

def choose_table():
    tables = list(TABLE_WEIGHTS.keys())
    weights = list(TABLE_WEIGHTS.values())
    return random.choices(tables, weights=weights, k=1)[0]

def generate_record():
    init_tables()
    table = choose_table()
    generator = TABLE_GENERATORS[table]
    record = generator()
    return record

@app.get("/") # HTTP endpoint method GET, URL /
async def single_record():
    return generate_record()

@app.get("/record/{count}") # HTTP endpoint method GET, URL /record/100 say
async def stream_records(count: int):
    async def event_generator():
        yield { # schema first
            "event": "schema",
            "data": json.dumps(uni_schema)
        }
        await asyncio.sleep(0.1)
        for _ in range(count): # then records
            await asyncio.sleep(0.01)
            yield {"event": "record", "data": json.dumps(generate_record())}
    return EventSourceResponse(event_generator())