"""
DESCRIPTION:

simulation of user behaviour
assumes a certain schema, not revealed to the server
data is streamed according to that

the server does not know the schema, so, to the server it seems like fields being random
which fields are included and excluded is based on random choice of table, again, unknown to server

username and metadata are added at the end
username will go with every data entry
metadata is added with a probability of 0.4

one important change could be to send data from multiple tables at once
for example, a combined record of instructor+department details
should we do that?

TO BE CHANGED AFTER CRUD DECISION:

currently, the only type of request is create or C
it will be easy to alter to add R, U and D since we're storing generated data in pools
can do that once CRUD team decides on input format
will also have to edit Runner.py and Network.py in tandem for the same, again, minor changes

HOW TO UNDERSTAND THIS CODE:

most of it is just generation of the fields in uni schema
important stuff starts after TABLE_GENERATORS dictionary
even the CRUD input edits will mostly go there
"""

from fastapi import FastAPI
from faker import Faker
from sse_starlette.sse import EventSourceResponse
import random
import asyncio
import json

app = FastAPI() # app
faker = Faker() # faker generator

DEPT_POOL = []
COURSE_POOL = []
CLASSROOM_POOL = []
TIMESLOT_POOL = []
SECTION_POOL = []
STUDENT_POOL = []
INSTRUCTOR_POOL = []

initiated_tabs = False

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

USER_POOL = [faker.user_name() for _ in range(20)] # adding username is mandatory

def choose_table(): # to randomise fields being sent for data entry
    tables = list(TABLE_WEIGHTS.keys())
    weights = list(TABLE_WEIGHTS.values())
    return random.choices(tables, weights=weights, k=1)[0]

def get_nested_metadata(): # to generate metadata
    full_meta = { # full potential structure
        "sensor_data": {
            "version": "2.1",
            "calibrated": random.choice([True, False])
        },
        "tags": [faker.word() for _ in range(random.randint(1, 3))]
    }
    sparse_meta = {k: v for k, v in full_meta.items() if random.random() > 0.5} # 50% chance to drop each key in the nested object
    return sparse_meta if sparse_meta else None # None if noe field meaning empty

def generate_record():
    table = choose_table()
    generator = TABLE_GENERATORS[table]
    record = generator()
    record["username"] = random.choice(USER_POOL) # username is mandatory
    if random.random() > 0.5: # 50 pc chance of adding metadata
        meta_content = get_nested_metadata()
        record["metadata"] = meta_content
    return record

@app.get("/") # HTTP endpoint method GET, URL /
async def single_record():
    return generate_record()

@app.get("/record/{count}") # HTTP endpoint method GET, URL /record/100 say
async def stream_records(count: int):
    async def event_generator():
        for _ in range(count): # then records
            await asyncio.sleep(0.01)
            rec = generate_record()
            yield {"event": "record", "data": json.dumps(rec)}
    return EventSourceResponse(event_generator())
