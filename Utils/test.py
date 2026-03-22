"""
Run this from the same folder as schema_infere.py:
    python test_schema.py
"""

import json
from collections import deque
from schema_maker import SchemaInfere

# ── SSE data ─────────────────────────────────────────────────────────
RAW = """event: init
data: {"username": {"global_key": "true", "unique": "true"}, "student_id": {"unique": "true"}, "instructor_id": {"unique": "true"}, "time_slot_id": {"unique": "true"}, "course_id": {"unique": "true"}, "dept_name": {"unique": "true"}, "sec_id": {}, "title": {}, "credits": {}, "building": {}, "room_no": {}, "tot_cred": {}, "student_name": {}, "instructor_name": {}, "semester": {}, "year": {}, "day": {}, "start_time": {}, "end_time": {}, "capacity": {}, "grade": {}, "prereq_id": {}, "salary": {}, "budget": {}}

event: create
data: {"dept_name": "Geography", "building": "Michael Tower", "budget": 820852.6, "username": "loretta65"}
event: create
data: {"course_id": "CS1029", "title": "Low.", "dept_name": "Geography", "credits": 5, "username": "katherine37"}
event: create
data: {"course_id": "CS6074", "title": "Dinner glass.", "dept_name": "Geography", "credits": 2, "username": "loretta65"}
event: create
data: {"student_id": "S9915", "student_name": "Brandi Cannon", "dept_name": "Geography", "tot_cred": 23, "username": "loretta65"}
event: create
data: {"course_id": "CS1773", "title": "Likely fill poor.", "dept_name": "Geography", "credits": 4, "username": "wyang"}
event: create
data: {"time_slot_id": "T737", "day": "Monday", "start_time": "12:00", "end_time": "12:50", "username": "vlong"}
event: create
data: {"instructor_id": "I7306", "instructor_name": "Mckenzie Moore", "dept_name": "Geography", "salary": 667664.33, "username": "jodischneider"}
event: create
data: {"dept_name": "Geography", "building": "Mathis Center", "budget": 3153761.34, "instructor_id": "I2417", "instructor_name": "Jacqueline Adams", "salary": 987676.42, "username": "williamskaren"}
event: create
data: {"course_id": "CS0349", "title": "Same true.", "dept_name": "Geography", "credits": 1, "username": "sarah08"}
event: create
data: {"dept_name": "History", "building": "Baker Building", "budget": 4242312.32, "username": "loretta65"}
event: create
data: {"dept_name": "Biology", "building": "Cathy Tower", "budget": 3460819.32, "instructor_id": "I6737", "instructor_name": "Steven Thomas", "salary": 727333.51, "username": "wyang"}
event: create
data: {"instructor_id": "I0793", "instructor_name": "Sarah Page", "dept_name": "Geography", "salary": 988530.75, "username": "jodischneider"}
event: create
data: {"student_id": "S7581", "student_name": "Drew Johnson", "dept_name": "Geography", "tot_cred": 22, "username": "william11"}
event: create
data: {"student_id": "S2812", "student_name": "John Stein", "dept_name": "Biology", "tot_cred": 22, "username": "williamskaren"}
event: create
data: {"time_slot_id": "T630", "day": "Wednesday", "start_time": "12:00", "end_time": "12:50", "username": "vlong"}
event: create
data: {"course_id": "CS6554", "title": "Run serve my.", "dept_name": "Biology", "credits": 2, "username": "johngray"}
event: create
data: {"instructor_id": "I9110", "instructor_name": "Cody Simpson", "dept_name": "Geography", "salary": 997271.83, "username": "hernandezangela"}
event: create
data: {"student_id": "S0149", "student_name": "Jimmy Adams", "dept_name": "Geography", "tot_cred": 32, "username": "johnsonchristopher"}
event: create
data: {"student_id": "S6390", "student_name": "Carmen Collins", "dept_name": "Geography", "tot_cred": 23, "username": "connor93"}
event: create
data: {"building": "Rodriguez Center", "room_no": "R366", "capacity": 41, "username": "ljohnson"}
event: create
data: {"course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "building": "Rodriguez Center", "room_no": "R366", "time_slot_id": "T737", "username": "michelleromero"}
event: create
data: {"dept_name": "Geography", "building": "Ray Building", "budget": 2446644.65, "instructor_id": "I5332", "instructor_name": "Todd Ramirez", "salary": 727384.29, "username": "katherine37"}
event: create
data: {"instructor_id": "I9110", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "jbaker"}
event: create
data: {"dept_name": "Maths", "building": "Hobbs Building", "budget": 2376971.26, "instructor_id": "I1028", "instructor_name": "Darren Beasley", "salary": 750715.69, "username": "michelleromero"}
event: create
data: {"course_id": "CS2868", "title": "Likely fill poor.", "dept_name": "Geography", "credits": 1, "username": "jesus91"}
event: create
data: {"course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "building": "Leonard Center", "room_no": "R433", "time_slot_id": "T630", "username": "mcdowellsamuel"}
event: create
data: {"time_slot_id": "T876", "day": "Tuesday", "start_time": "15:00", "end_time": "15:50", "username": "hernandezangela"}
event: create
data: {"instructor_id": "I2362", "instructor_name": "Joseph Delgado", "dept_name": "Geography", "salary": 900922.63, "username": "johngray"}
event: create
data: {"course_id": "CS8157", "title": "All such face.", "dept_name": "Maths", "credits": 5, "username": "william11"}
event: create
data: {"building": "Mathis Center", "room_no": "R195", "capacity": 167, "username": "sarah08"}
event: create
data: {"instructor_id": "I0793", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "username": "jesus91"}
event: create
data: {"instructor_id": "I8153", "instructor_name": "Charles Bishop", "dept_name": "History", "salary": 221235.6, "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "username": "nhall"}
event: create
data: {"student_id": "S3158", "student_name": "Edward Cook", "dept_name": "Geography", "tot_cred": 19, "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "grade": "C", "username": "jesus91"}
event: create
data: {"student_id": "S3514", "student_name": "Jonathon Jimenez", "dept_name": "Geography", "tot_cred": 16, "username": "ljohnson"}
event: create
data: {"instructor_id": "I7306", "instructor_name": "Mckenzie Moore", "dept_name": "Biology", "salary": 539474.8, "username": "hernandezangela"}
event: create
data: {"course_id": "CS6758", "title": "Wife fall left.", "dept_name": "Biology", "credits": 4, "username": "johngray"}
event: create
data: {"instructor_id": "I1028", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "username": "jodischneider"}
event: create
data: {"instructor_id": "I0080", "instructor_name": "Melissa Robbins", "dept_name": "Biology", "salary": 775952.53, "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "william11"}
event: create
data: {"instructor_id": "I7306", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "bakerbrenda"}
event: create
data: {"time_slot_id": "T105", "day": "Tuesday", "start_time": "9:00", "end_time": "9:50", "username": "connor93"}
event: create
data: {"student_id": "S1095", "student_name": "Sara Davis", "dept_name": "Geography", "tot_cred": 32, "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "grade": "D", "username": "jbaker"}
event: create
data: {"student_id": "S3617", "student_name": "Matthew Stout", "dept_name": "Maths", "tot_cred": 29, "username": "johngray"}
event: create
data: {"student_id": "S2812", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "grade": "A", "username": "timothyholt"}
event: create
data: {"dept_name": "Arts", "building": "Shawn Tower", "budget": 4088982.47, "course_id": "CS6700", "title": "Painting situation.", "credits": 2, "username": "hernandezangela"}
event: create
data: {"instructor_id": "I7306", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "michelleromero"}
event: create
data: {"dept_name": "Maths", "building": "Baker Building", "budget": 2047689.86, "student_id": "S6320", "student_name": "Susan Mills", "tot_cred": 25, "username": "ljohnson"}
event: create
data: {"dept_name": "History", "building": "Hobbs Building", "budget": 1671435.98, "student_id": "S8440", "student_name": "Amanda Brown", "tot_cred": 29, "username": "wyang"}
event: create
data: {"building": "Leonard Center", "room_no": "R738", "capacity": 126, "username": "nhall"}
event: create
data: {"instructor_id": "I0793", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "bakerbrenda"}
event: create
data: {"course_id": "CS1466", "title": "Truth open fill.", "dept_name": "Geography", "credits": 4, "username": "johngray"}
event: create
data: {"time_slot_id": "T258", "day": "Friday", "start_time": "15:00", "end_time": "15:50", "username": "johnsonchristopher"}
event: create
data: {"student_id": "S3158", "instructor_id": "I0080", "username": "johngray"}
event: create
data: {"student_id": "S1118", "student_name": "Tina Mullen", "dept_name": "Maths", "tot_cred": 23, "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "A", "username": "nhall"}
event: create
data: {"student_id": "S6390", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "B+", "username": "timothyholt"}
event: create
data: {"course_id": "CS6516", "title": "Interesting.", "dept_name": "Geography", "credits": 2, "prereq_id": ["CS8157", "CS6700", "CS1029"], "username": "mcdowellsamuel"}
event: create
data: {"student_id": "S6390", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "grade": "C", "username": "timothyholt"}
event: create
data: {"instructor_id": "I2362", "instructor_name": "Joseph Delgado", "dept_name": "Maths", "salary": 192342.02, "username": "jesus91"}
event: create
data: {"student_id": "S6320", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "A-", "username": "ljohnson"}
event: create
data: {"course_id": "CS1466", "sec_id": "SEC36", "semester": "Summer", "year": 2022, "building": "Leonard Center", "room_no": "R738", "time_slot_id": "T258", "username": "michelleromero"}
event: create
data: {"building": "Mathis Center", "room_no": "R708", "capacity": 153, "username": "williamskaren"}
event: create
data: {"course_id": "CS8147", "title": "Soldier behind.", "dept_name": "Geography", "credits": 1, "username": "williamskaren"}
event: create
data: {"course_id": "CS4803", "title": "Scene away.", "dept_name": "Maths", "credits": 1, "prereq_id": ["CS6554", "CS1029", "CS6516"], "username": "bakerbrenda"}
event: create
data: {"student_id": "S7242", "student_name": "Nancy Clay", "dept_name": "Maths", "tot_cred": 29, "username": "vlong"}
event: create
data: {"course_id": "CS7577", "title": "Concern our.", "dept_name": "Maths", "credits": 1, "prereq_id": ["CS0349", "CS1773", "CS8147"], "username": "vlong"}
event: create
data: {"course_id": "CS1689", "title": "Data.", "dept_name": "Maths", "credits": 2, "prereq_id": ["CS1773", "CS8147", "CS6554"], "username": "hernandezangela"}
event: create
data: {"instructor_id": "I5332", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "username": "ljohnson"}
event: create
data: {"student_id": "S2569", "student_name": "Savannah Jones", "dept_name": "History", "tot_cred": 18, "course_id": "CS1466", "sec_id": "SEC36", "semester": "Summer", "year": 2022, "grade": "D", "username": "katherine37"}
event: create
data: {"instructor_id": "I6098", "instructor_name": "Carrie Jackson", "dept_name": "Geography", "salary": 238702.01, "username": "williamskaren"}
event: create
data: {"dept_name": "History", "building": "Miguel Center", "budget": 556070.16, "course_id": "CS3911", "title": "Be suggest.", "credits": 2, "username": "jesus91"}
event: create
data: {"instructor_id": "I8672", "instructor_name": "Zachary Bell", "dept_name": "Arts", "salary": 703098.78, "username": "johngray"}
event: create
data: {"student_id": "S3514", "course_id": "CS1466", "sec_id": "SEC36", "semester": "Summer", "year": 2022, "grade": "A", "username": "sarah08"}
event: create
data: {"course_id": "CS6479", "title": "Response in including.", "dept_name": "Biology", "credits": 4, "username": "jodischneider"}
event: create
data: {"course_id": "CS8323", "title": "Guess son under always.", "dept_name": "Biology", "credits": 2, "username": "hernandezangela"}
event: create
data: {"student_id": "S1118", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "A", "username": "ljohnson"}
event: create
data: {"dept_name": "Geography", "building": "Samantha Hall", "budget": 4619312.0, "student_id": "S8529", "student_name": "Bryan Miller", "tot_cred": 27, "username": "mcdowellsamuel"}
event: create
data: {"instructor_id": "I8153", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "jbaker"}
event: create
data: {"time_slot_id": "T400", "day": "Monday", "start_time": "12:00", "end_time": "12:50", "username": "timothyholt"}
event: create
data: {"instructor_id": "I7306", "course_id": "CS1466", "sec_id": "SEC36", "semester": "Summer", "year": 2022, "username": "johngray"}
event: create
data: {"course_id": "CS2103", "title": "Tonight generation.", "dept_name": "Geography", "credits": 3, "username": "connor93"}
event: create
data: {"dept_name": "Biology", "building": "Shannon Complex", "budget": 3359197.31, "course_id": "CS8815", "title": "Same true.", "credits": 2, "username": "bakerbrenda"}
event: create
data: {"student_id": "S0149", "instructor_id": "I9110", "username": "jesus91"}
event: create
data: {"instructor_id": "I7306", "instructor_name": "Mckenzie Moore", "dept_name": "Biology", "salary": 556499.25, "username": "william11"}
event: create
data: {"course_id": "CS3275", "title": "Task market morning.", "dept_name": "Geography", "credits": 1, "username": "williamskaren"}
event: create
data: {"student_id": "S4008", "student_name": "Jennifer Rush", "dept_name": "Geography", "tot_cred": 19, "username": "jbaker"}
event: create
data: {"student_id": "S7416", "student_name": "Ronald Nguyen", "dept_name": "Biology", "tot_cred": 24, "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "B", "username": "loretta65"}
event: create
data: {"dept_name": "Biology", "building": "Rogers Center", "budget": 3765490.41, "instructor_id": "I6737", "instructor_name": "Steven Thomas", "salary": 779554.72, "username": "ljohnson"}
event: create
data: {"course_id": "CS6950", "title": "Dinner glass.", "dept_name": "Arts", "credits": 2, "username": "jbaker"}
event: create
data: {"student_id": "S0581", "student_name": "Kelly Herring", "dept_name": "Biology", "tot_cred": 19, "username": "timothyholt"}
event: create
data: {"dept_name": "Geography", "building": "Cathy Tower", "budget": 4538598.71, "instructor_id": "I4163", "instructor_name": "Lawrence Morris", "salary": 103154.57, "username": "hernandezangela"}
event: create
data: {"instructor_id": "I3012", "instructor_name": "David May", "dept_name": "Biology", "salary": 607954.88, "username": "wyang"}
event: create
data: {"building": "Martin Hall", "room_no": "R565", "capacity": 37, "username": "jesus91"}
event: create
data: {"course_id": "CS7502", "title": "Cause first.", "dept_name": "Arts", "credits": 4, "prereq_id": ["CS6516", "CS7577", "CS1689"], "username": "wyang"}
event: create
data: {"instructor_id": "I6879", "instructor_name": "James Collins", "dept_name": "Arts", "salary": 668467.11, "username": "johngray"}
event: create
data: {"student_id": "S7581", "course_id": "CS2868", "sec_id": "SEC09", "semester": "Spring", "year": 2024, "grade": "A", "username": "hernandezangela"}
event: create
data: {"instructor_id": "I5332", "instructor_name": "Todd Ramirez", "dept_name": "Geography", "salary": 332689.79, "username": "mcdowellsamuel"}
event: create
data: {"dept_name": "Biology", "building": "Hobbs Building", "budget": 2818865.97, "student_id": "S2654", "student_name": "Kayla Anderson", "tot_cred": 20, "username": "katherine37"}
event: create
data: {"course_id": "CS8717", "title": "Hit it seat spend.", "dept_name": "History", "credits": 5, "username": "mcdowellsamuel"}
event: create
data: {"dept_name": "Geography", "building": "Hobbs Building", "budget": 1821410.06, "instructor_id": "I9743", "instructor_name": "Aaron Glass", "salary": 753240.73, "username": "williamskaren"}
event: create
data: {"course_id": "CS8171", "title": "Best parent person.", "dept_name": "Biology", "credits": 3, "prereq_id": ["CS6554", "CS2103", "CS8717"], "username": "michelleromero"}
event: create
data: {"instructor_id": "I7306", "course_id": "CS0349", "sec_id": "SEC37", "semester": "Fall", "year": 2021, "username": "johnsonchristopher"}"""


# ── Parse SSE ─────────────────────────────────────────────────────────
def parse_sse(text):
    init_config = None
    records = deque()
    current_event = None
    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith("event:"):
            current_event = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            payload = json.loads(line[5:].strip())
            if current_event == "init":
                init_config = payload
            elif current_event == "create":
                records.append(payload)
    return init_config, records


init_config, records = parse_sse(RAW)

global_key    = next(k for k, v in init_config.items() if v.get("global_key") == "true")
unique_fields = [k for k, v in init_config.items()
                 if v.get("unique") == "true" and k != global_key]

print(f"Global key   : {global_key}")
print(f"Unique fields: {unique_fields}")
print(f"Total records: {len(records)}")

# Convert plain-dict queue to (event, record) tuples and append extra events
from collections import deque as _dq
records = _dq(("create", r) for r in records)
extra = [
    ("get",    {"student_id": "S1119", "username": "michaelcooper", "COLUMNS": ["instructor_id"]}),
    ("add",    {"time_slot_id": "T601", "day": "Friday", "start_time": "9:00", "end_time": "9:50", "username": "brittanybailey"}),
    ("remove", {"time_slot_id": "T776", "username": "jwilliams"}),
    ("change", {"course_id": "CS9662", "credits": 1, "username": "nberry"}),
]
records.extend(extra)
print(f"Total items (with extra events): {len(records)}")
print()

# ── Run engine ────────────────────────────────────────────────────────
engine = SchemaInfere(
    unique_fields=unique_fields,
    global_key=global_key,
    output_dir="."
)
schema = engine.queue_reader(records)

# ── Print schema ──────────────────────────────────────────────────────
print("\n=== TABLES ===")
for tname, tdef in schema["tables"].items():
    pk   = tdef.get("primary_key", "composite")
    cols = tdef["columns"]
    fks  = tdef["foreign_keys"]
    tag  = " [junction]" if tdef.get("is_junction") else ""
    print(f"\n  {tname}{tag}")
    print(f"    PK   : {pk}")
    print(f"    cols : {cols}")
    if fks:
        for fk in fks:
            print(f"    FK   : {fk}")

print("\n=== FUNCTIONAL DEPENDENCIES ===")
for key, deps in schema["functional_dependencies"].items():
    print(f"  {key} -> {deps}")

print("\n=== FOREIGN KEYS ===")
if schema["foreign_keys"]:
    for fk in schema["foreign_keys"]:
        print(f"  {fk}")
else:
    print("  (none detected)")

print("\n=== MANY-TO-MANY ===")
print(f"  {schema['many_to_many']}")

print("\n=== FIRST 10 OPERATIONS FROM SSE DATA ===")
import json as _j
with open("operations.log") as f:
    for i, line in enumerate(f):
        if i >= 10:
            break
        print(f"  {line.strip()}")

print("\nDone.")
print("  schema.json      -> full inferred schema")
print("  operations.log   -> one operation per line for every SSE record")