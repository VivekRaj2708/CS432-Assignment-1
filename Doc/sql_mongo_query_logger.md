# SQL & Mongo Logger Documentation

## Overview

After `Resolve` and `MapRegister` generate structured operations (`CREATE`, `ALTER`, `INSERT`), we built two logger scripts:

- `sql_logger.py`
- `mongo_logger.py`

Their purpose is to:  
Take the final `INSERT` operations from the queue and convert them into executable database queries.



# SQL Logger (`sql_logger.py`)

## Purpose

This script converts `INSERT` updates into MySQL-compatible SQL statements and writes them into `queries.log`.

It ensures that:

- Tables are created if they don’t exist
- New columns are added dynamically
- Inserts never fail due to missing columns



## How It Works

### 1. Table Creation

When a table appears for the first time:

```sql
CREATE TABLE IF NOT EXISTS table_name (...);
```

All columns are created as `TEXT` to avoid type conflicts.


### 2. Handling New Columns

If a new column appears later in the stream:

```sql
ALTER TABLE table_name ADD COLUMN new_column TEXT;
```

This guarantees that the script runs fully in MySQL Workbench without errors.


### 3. Insert Statements

Each update is converted into:

```sql
INSERT INTO table_name (col1, col2, ...)
VALUES (val1, val2, ...);
```

## Value Handling

Python values are converted safely:

- `None` → `NULL`
- `True/False` → `1/0`
- Numbers → kept numeric
- Lists & dicts → stored as JSON strings
- Strings → properly escaped


## Why Everything is TEXT

We chose `TEXT` for all columns because:

- The data stream is heterogeneous
- Types may change
- It prevents runtime errors
- It keeps ingestion fully adaptive

For this assignment, flexibility is more important than strict typing.


# Mongo Logger (`mongo_logger.py`)

## Purpose

This script converts `INSERT` updates into MongoDB `insertOne()` commands and writes them into `mongo_queries.log`.


## How It Works

For every `INSERT` update:

```javascript
db.collection_name.insertOne({...});
```

No schema creation or alteration is needed.

MongoDB automatically:

- Creates collections
- Allows new fields
- Handles different document shapes


## Value Handling

- `None` → `null`
- `True/False` → `true/false`
- Numbers → numbers
- Lists → arrays
- Dicts → embedded documents
- Strings → quoted


# Overall Flow

```
JSON Stream
   ↓
Resolve (type inference)
   ↓
MapRegister (schema management)
   ↓
Update Queue (CREATE / ALTER / INSERT)
   ↓
SQL Logger OR Mongo Logger
   ↓
Executable database queries
```


## Final Outcome

- SQL output runs directly in MySQL Workbench.
- Mongo output runs directly in MongoDB shell or Compass.
- Both handle dynamic and heterogeneous data safely.
- Schema logic and database logic are cleanly separated.
