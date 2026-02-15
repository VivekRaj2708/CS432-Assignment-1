# CS 432 Assignment 1 Report

# Task: Adaptive Ingestion & Hybrid Backend Placement

## Github Repository Link
[Link to Repository](https://github.com/VivekRaj2708/CS432-Assignment-1)

## Team Members: 
- Harinarayan J (23110128) 
- Nupoor Assudani (23110224)  
- Pavitr Chandwani (23110241)  
- Pranav Thakkar (23110253) 
- Vivek Raj (23110362)

## Project Overview:
This project implements an autonomous data ingestion system that dynamically decides whether incoming JSON data is better suited for MySQL or MongoDB.

Our code automatically decides where to store each record based on certain heuristics that we developed which we discuss below.

## Our Approach:
JSON Stream (with logging) --> Cleaning of the data --> Mapping of Data --> Datatype handling --> Decision of Database based on heuristics --> Ingestion

### Workflow of files:
The module `Network.py` is an asynchronous GET wrapper that returns the parsed JSON stream or raises errors on HTTP, as we are using HTTPX to get the stream records asynchronously using the `stream_sse_records` function. It cleans the data and also attaches the required timestamp with as asked in the assignment and in the end it rpovides a queue with the following record. It also logs any errors in a file (if any), and also provides control to the user to set the maximum size of queue to handle increased load.

The module `Resolve.py` implements a dynamic metadata and type-resolution system for ingestion. The **Metadata** class performs automatic type inference, and safe value conversions as new records are processed. Initially, an 'UNK' datatype is assigned and then it's updated to the appropriate data type using a proritized inference strategy. This system supports types like **int**,**float**,**bool**,**str**, as well as subtype tracking like **list<int>**. The module also supports auto-increment feature. This mainly handles the ingestion part for the SQL database.

The module `MapRegister.py` implements a mapping engine that transforms nested JSON records into normalized relational representation. It maintains a column metadata using the previously defined datatype inference system. For every record, the system recursively processes nested objects and lists, automatically creating tables and establishing keys and identifiers. It generates a queue of database operations like **CREATE**,**ALTER**,**INSERT**, etc. The schema state can be serialized and restored using pickle-based persistance allowing the pipeline to maintain continuity. 

The module `Log.py` configures a centralized logging system for the pipeline. A root logger is initialized with an INFO severity level to write timestamped entries. The logging includes the log level, timestamp, and a descriptive message, helpful in closely monitoring the system and effective debugging if need be. 

The module `sql_logger.py` translates the generated operation queue to executable SQL statements. It has functions to safely convert Python values to SQL literals, while ensuring proper handling of NULL values, booleans, numeric types, etc. It automatically generates **CREATE TABLE**, **ALTER TABLE**, and **INSERT** statements based on the recieved data. By keeping track in a log file, the system prevents duplicate schema operations. 

The module `mongo_logger.py` generates MongoDB insert statements from the database opeartion queue, enabling documented oriented storage alongside relational persistence. Python values are convrted to MongoDB compatible JSON representations, ensuring correct handling of data. Since MongooDB is schema-less, it only needs insert statements, simplifying the generation statements compared to SQL. It also writes executable MongoDB commands to a log file, providing ready to run file for ingestion. 

### Heuristics we decided on:

### Workflow Diagram:

### Conclusive Workflow:

### Individual Documentation:

Comprehensive documentation is available in the `Doc/` directory:

- **[Network Module](Doc/Network.md)** - SSE streaming, HTTP clients, queue management
- **[Resolver Module](Doc/Resolver.md)** - Type resolution, schema inference, SQL operations
- **[SQL and MongoDB Logger Module](Doc/Logger.md)** - Queries detection from database queue, Query creation, Executable statements creation

## Project Structure:

```
CS432-Assignment-1/
├── Utils/               # Core utility modules
│   ├── Network.py      # SSE streaming and HTTP client
│   ├── Resolve.py      # Type resolution and metadata management
│   ├── MapRegister.py  # Schema registry and SQL operation generation
│   ├── SQL.py          # SQL query generation utilities
│   ├── Log.py          # Logging configuration
│   ├── BiTemporal.py   # Timestamp record
│   └── Classify.py     # Database classifier
├── tests/              # Test suite
│   ├── test_network.py    # Network streaming tests
│   ├── test_join.py       # Join tests
│   └── test_resolve.py    # Type resolution tests
├── Doc/                # Documentation
│   ├── Network.md      # Network module documentation
│   ├── Resolver.md     # Resolve & MapRegister documentation
│   └── Logger.md     # SQL & MongoDB logger documentation
├── T2/                 # Simulation module
│   └── simulation.py  # Simulation utilities
├── Runner.py           # Main application entry point
├── server.ps1/sh       # Development server scripts
├── setup.ps1/sh        # Environment setup scripts
├── requirements.txt    # Python dependencies
├── mongo_logger.py     # MongoDB statement generation
├── sql_logger.py       # SQL query generation
├── logs.log            # Main logs file
└── Assignment_Report.md # Detailed assignment report
```

## Setup and Starting of system:

### Starting the Server

**Windows:**
```powershell
.\server.ps1
```

**Linux/macOS:**
```bash
chmod +x server.sh
./server.sh
```

The server will start on `http://127.0.0.1:8000` by default.

### Running the Main Application

```bash
python Runner.py
```
