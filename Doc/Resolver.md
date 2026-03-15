# Resolve & MapRegister Documentation

## Overview
The `Resolve` and `MapRegister` modules work together to provide dynamic schema inference and type resolution for JSON data streams. They automatically detect data types, handle type transitions, and generate SQL-like operations (CREATE, ALTER, INSERT) for database synchronization.

**Key Features:**
- Automatic type inference from values
- Graceful type transitions with configurable rules
- Nested dictionary and list handling
- Auto-increment primary key generation
- SQL operation queue generation
- Persistent schema storage

---

## Resolve Module

### Helper Functions

#### `CheckBool(value)`
Determines if a value can be converted to a boolean.

**Parameters:**
- `value`: Any value to check

**Returns:**
- `bool`: `True` if convertible to boolean, `False` otherwise

**Accepted formats:**
- `bool` types
- Strings: `'true'`, `'1'`, `'yes'`, `'false'`, `'0'`, `'no'` (case-insensitive)
- Integers: `0`, `1`

---

#### `ResolveBool(value)`
Converts a value to boolean.

**Parameters:**
- `value`: Value to convert

**Returns:**
- `bool`: Resolved boolean value

**Raises:**
- `ValueError`: If value cannot be converted to boolean

**Examples:**
```python
ResolveBool('yes')    # True
ResolveBool('0')      # False
ResolveBool(1)        # True
ResolveBool('maybe')  # ValueError
```

---

### Metadata Class

Represents a column's type metadata with automatic type resolution and transition capabilities.

#### `__init__(type_="UNK", auto=False)`
Initialize a Metadata instance.

**Parameters:**
- `type_` (str): Initial type. Options: `"int"`, `"str"`, `"float"`, `"bool"`, `"list"`, `"UNK"`
- `auto` (bool): Enable auto-increment (only for `"int"` type)

**Example:**
```python
# Auto-increment integer (e.g., primary key)
id_meta = Metadata(type_="int", auto=True)

# Unknown type (inferred from first value)
field_meta = Metadata(type_="UNK")

# Fixed string type
name_meta = Metadata(type_="str")
```

---

#### `resolveValue(value=None, queue=None, column_name=None)`
Resolves a value to the metadata's current type, transitioning types if necessary.

**Parameters:**
- `value`: Value to resolve
- `queue` (deque, optional): Queue to append ALTER operations when type changes
- `column_name` (str, optional): Column name for ALTER operations

**Returns:**
- Resolved value in the appropriate type

**Behavior:**
1. **Auto-increment**: If `auto=True`, increments and returns the counter (ignores `value`)
2. **UNK type**: Infers type from the first value
3. **Type mismatch**: Attempts allowed type transitions
4. **Type change**: Emits ALTER operation to queue if provided

**Example:**
```python
from collections import deque

meta = Metadata(type_="int")
queue = deque()

# Normal resolution
result = meta.resolveValue("42", queue=queue, column_name="age")
# result = 42, meta.type = "int"

# Type transition: int → float
result = meta.resolveValue("3.14", queue=queue, column_name="age")
# result = 3.14, meta.type = "float"
# queue contains: {"type": "ALTER", "column_name": "age", "old_type": "int", "new_type": "float"}
```

---

#### Type Transition Rules

Each type can transition to specific target types when direct conversion fails:

| Current Type | Allowed Transitions |
|--------------|---------------------|
| `int` | `float`, `list<int>`, `list<float>`, `str` |
| `float` | `list<float>`, `str` |
| `bool` | `int`, `float`, `list<int>`, `list<float>`, `str` |
| `str` | `list<str>` |
| `list` | Subtype transitions (see below) |

**List Subtype Transitions:**

| Current Subtype | Allowed Subtypes |
|-----------------|------------------|
| `int` | `int`, `float`, `str` |
| `float` | `float`, `str` |
| `bool` | `bool`, `int`, `float`, `str` |
| `str` | `str` |

**Important:** Type transitions only occur after attempting to resolve to the current type. For example:
- `int` type with value `"5"` → stays `int`, returns `5`
- `int` type with value `"5.5"` → transitions to `float`, returns `5.5`

---

#### `convert_scalar(target_type, value)`
Converts a single value to a target scalar type.

**Parameters:**
- `target_type` (str): Target type (`"int"`, `"float"`, `"bool"`, `"str"`)
- `value`: Value to convert

**Returns:**
- Converted value

**Raises:**
- `ValueError`: If conversion fails

**Special Rules:**
- `int` conversion rejects non-integer floats (e.g., `1.5` → ValueError)
- `bool` conversion uses `ResolveBool()` logic

---

#### `convert_list(subtype, value)`
Converts all elements in a list to a target subtype.

**Parameters:**
- `subtype` (str): Element type
- `value`: List or list-like string (e.g., `"[1, 2, 3]"`)

**Returns:**
- List of converted values

**Example:**
```python
meta = Metadata(type_="list")
result = meta.convert_list("int", "[1, 2, 3]")
# result = [1, 2, 3]
```

---

#### `normalize_list(value)`
Normalizes input to a Python list.

**Parameters:**
- `value`: List object or JSON/Python string representation

**Returns:**
- Python `list` object

**Behavior:**
- If string: tries `json.loads()`, falls back to `ast.literal_eval()`
- If already list: returns as-is
- Otherwise: raises `ValueError`

---

#### `try_allowed_transitions(value, allowed_targets, queue=None, column_name=None)`
Attempts to convert value using a list of allowed target types.

**Parameters:**
- `value`: Value to convert
- `allowed_targets` (list): List of type strings (e.g., `["float", "list:int", "str"]`)
- `queue` (deque, optional): Queue for ALTER operations
- `column_name` (str, optional): Column name for ALTER operations

**Returns:**
- Converted value (uses first successful transition)

**Raises:**
- `ValueError`: If all transitions fail

**Behavior:**
- Tries each target in order
- Updates `self.type` and `self.subtype` on success
- Emits ALTER operation if queue is provided
- Targets prefixed with `list:` are treated as list types

---

#### `get_allowed_list_subtypes()`
Returns allowed subtype transitions for the current list subtype.

**Returns:**
- `list` of allowed subtype strings

**Example:**
```python
meta = Metadata(type_="list")
meta.subtype = Metadata(type_="int")
meta.get_allowed_list_subtypes()
# ["int", "float", "str"]
```

---

#### `__repr__()`
String representation of metadata.

**Returns:**
- Human-readable string describing the type

**Examples:**
```python
str(Metadata(type_="int"))
# "Metadata(type=int)"

str(Metadata(type_="int", auto=True))
# "Metadata(type=int, AUTO)"

meta = Metadata(type_="list")
meta.subtype = Metadata(type_="str")
str(meta)
# "Metadata(array<Metadata(type=str)>)"
```

---

## MapRegister Class

Manages a dynamic schema registry for nested JSON structures. Maps keys to `Metadata` instances or nested `MapRegister` instances.

### `__init__(table_name="root")`
Initialize a MapRegister instance.

**Parameters:**
- `table_name` (str): Name for this table/registry level

**Behavior:**
- Creates a built-in `table_autogen_id` column with auto-increment

**Example:**
```python
registry = MapRegister(table_name="users")
```

---

### `ResolveRequest(request, updateOrder=None)`
Processes a dictionary request, updating schema and generating SQL operations.

**Parameters:**
- `request` (dict): Dictionary to resolve
- `updateOrder` (deque, optional): Queue to append SQL-like operations

**Returns:**
- `int`: Auto-generated ID for this request

**Generated Operations:**
1. **CREATE**: When a nested dict creates a new child table
2. **ALTER**: When a column is added or type changes
3. **INSERT**: At the end of processing, with all columns and values

**Operation Format:**

```python
# CREATE
{
    "type": "CREATE",
    "table_name": "root_users_address",
    "table_map": <MapRegister instance>
}

# ALTER (new column)
{
    "type": "ALTER",
    "table_name": "root_users",
    "column_name": "age",
    "old_type": None,
    "new_type": "int"
}

# ALTER (type change)
{
    "type": "ALTER",
    "table_name": "root_users",
    "column_name": "score",
    "old_type": "int",
    "new_type": "float"
}

# INSERT
{
    "type": "INSERT",
    "table_name": "root_users",
    "columns": ["name", "age", "address"],
    "values": ["Alice", 30, 1]  # 1 is the foreign key to address table
}
```

**Example:**
```python
from collections import deque

registry = MapRegister(table_name="users")
queue = deque()

# First request
registry.ResolveRequest({"name": "Alice", "age": 30}, updateOrder=queue)
# queue contains:
# - ALTER for "name" column (new)
# - ALTER for "age" column (new)
# - INSERT with columns=["name", "age"], values=["Alice", 30]

# Second request with nested dict
registry.ResolveRequest({
    "name": "Bob",
    "age": 25,
    "address": {"city": "NYC", "zip": "10001"}
}, updateOrder=queue)
# queue additionally contains:
# - CREATE for "users_address" table
# - ALTER for "city" column
# - ALTER for "zip" column
# - INSERT for "users_address" table
# - ALTER for "address" column
# - INSERT for "users" table with address foreign key
```

---

<!-- ### `resolve_nested_list(key, items, updateOrder=None)` -->
Recursively resolves lists containing dictionaries or nested lists.

**Parameters:**
- `key` (str): Column name for the list
- `items` (list): List items to resolve
- `updateOrder` (deque, optional): Queue for SQL operations

**Behavior:**
- Dicts in list → creates/updates child MapRegister
- Lists in list → recursively processes
- Each dict becomes a separate INSERT in the child table

**Example:**
```python
registry = MapRegister(table_name="orders")
queue = deque()

registry.ResolveRequest({
    "order_id": 1,
    "items": [
        {"product": "Widget", "qty": 5},
        {"product": "Gadget", "qty": 2}
    ]
}, updateOrder=queue)
# Creates "orders_items" child table
# Inserts 2 rows into "orders_items"
```

---

### Special Methods

#### `__getitem__(key)`
Access metadata/child registry by key.

```python
registry["name"]  # Returns Metadata instance for "name" column
```

#### `__contains__(key)`
Check if key exists in registry.

```python
"name" in registry  # True/False
```

#### `__iter__()`
Iterate over keys.

```python
for key in registry:
    print(key, registry[key])
```

#### `__repr__()`
Tabulated representation of the registry.

**Returns:**
- Formatted table string showing all columns and their metadata

**Example:**
```python
print(registry)
# +---------------------+----------------------+
# | Key                 | Metadata             |
# +=====================+======================+
# | table_autogen_id    | Metadata(type=int, AUTO) |
# +---------------------+----------------------+
# | name                | Metadata(type=str)   |
# +---------------------+----------------------+
# | age                 | Metadata(type=int)   |
# +---------------------+----------------------+
```

---

### Persistence

#### `Save(filename=None)`
Serializes the registry to a pickle file.

**Parameters:**
- `filename` (str, optional): Path to save file. Defaults to `"map_register.pkl"`

**Example:**
```python
registry.Save("schema.pkl")
```

---

#### `Load(filename=None)`
Loads a registry from a pickle file.

**Parameters:**
- `filename` (str, optional): Path to load file. Defaults to `"map_register.pkl"`

**Example:**
```python
registry.Load("schema.pkl")
```

---

## Integration Example

Complete example showing Resolve and MapRegister working together:

```python
from collections import deque
from Utils.MapRegister import MapRegister

# Create a registry for user data
registry = MapRegister(table_name="users")
queue = deque()

# Process first request
registry.ResolveRequest({
    "name": "Alice",
    "age": 30,
    "score": 95
}, updateOrder=queue)

# Process into: queue contains ALTER (name), ALTER (age), ALTER (score), INSERT

# Process second request with type change
registry.ResolveRequest({
    "name": "Bob",
    "age": 25,
    "score": 87.5  # int → float transition
}, updateOrder=queue)

# Process into: queue additionally contains ALTER (score type change), INSERT

# Process request with nested object
registry.ResolveRequest({
    "name": "Charlie",
    "age": 35,
    "score": 92.0,
    "address": {
        "street": "123 Main St",
        "city": "Boston"
    }
}, updateOrder=queue)

# Process into: queue additionally contains CREATE (users_address), ALTER (street), 
# ALTER (city), INSERT (users_address), ALTER (address), INSERT (users)

# Inspect the queue
while queue:
    operation = queue.popleft()
    print(f"{operation['type']}: {operation}")

# Save the schema for later use
registry.Save("user_schema.pkl")
```

---

## Best Practices

1. **Always provide `updateOrder` queue** when you need to track schema changes
2. **Use meaningful `table_name`** for top-level registries (defaults to `"root"`)
3. **Provide `column_name` to `resolveValue()`** when tracking column-level type changes
4. **Process queue in order** to maintain correct CREATE → ALTER → INSERT sequencing
5. **Save schemas periodically** to avoid reprocessing on restart
6. **Load existing schemas first** before processing new data to maintain schema consistency

---

## Type Safety Considerations

- **Non-integer floats** cannot convert to `int` (e.g., `1.5` → `int` fails, transitions to `float`)
- **String to list** only transitions if value is JSON/Python list format
- **Type downgrades** not allowed (e.g., `float` → `int` not automatic)
- **AUTO columns** ignore input values, always increment
- **Empty lists** for UNK type cannot infer subtype (logs warning)

---

## Performance Notes

- Type inference is lazy (happens on first value)
- Type transitions attempt cheapest conversions first
- Nested structures create O(n) registries where n = nesting depth
- Queue operations are O(1) append
- Pickle serialization includes full schema tree