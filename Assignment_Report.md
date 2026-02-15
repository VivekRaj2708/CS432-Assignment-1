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

The module `BiTemporal.py` gives the system timestamps to create the bitemporal timestamps as required.

The module `Classify.py` acts as an adaptive routing engine that categorizes incoming data fields for optimal storage in either a SQL or MongoDB. It continuously observes the data stream and determines the storage destination based on the heuristics specified. The module persists its learned state across sessions, allowing the pipeline to automatically adapt its schema routing and issue reclassification events as the shape and consistency of the data evolve.

The module `MapRegister.py` implements a mapping engine that transforms nested JSON records into normalized relational representation. It maintains a column metadata using the previously defined datatype inference system. For every record, the system recursively processes nested objects and lists, automatically creating tables and establishing keys and identifiers. It generates a queue of database operations like **CREATE**,**ALTER**,**INSERT**, etc. The schema state can be serialized and restored using pickle-based persistance allowing the pipeline to maintain continuity. 

The module `Log.py` configures a centralized logging system for the pipeline. A root logger is initialized with an INFO severity level to write timestamped entries. The logging includes the log level, timestamp, and a descriptive message, helpful in closely monitoring the system and effective debugging if need be. 

The module `sql_logger.py` translates the generated operation queue to executable SQL statements. It has functions to safely convert Python values to SQL literals, while ensuring proper handling of NULL values, booleans, numeric types, etc. It automatically generates **CREATE TABLE**, **ALTER TABLE**, and **INSERT** statements based on the recieved data. By keeping track in a log file, the system prevents duplicate schema operations. 

The module `mongo_logger.py` generates MongoDB insert statements from the database opeartion queue, enabling documented oriented storage alongside relational persistence. Python values are convrted to MongoDB compatible JSON representations, ensuring correct handling of data. Since MongooDB is schema-less, it only needs insert statements, simplifying the generation statements compared to SQL. It also writes executable MongoDB commands to a log file, providing ready to run file for ingestion. 



## Report Questions:

### Normalization Strategy

### Placement Heuristics:

We have used the following four heuristics to classify the storage destination.

1. Nesting:  SQL is designed for flat, two-dimensional data, and it will require complex operations such as creating child tables, etc. We have directly routed nested fields to MongoDB.

2. Sparsity: The performance of SQL degrades with NULL heavy data as they use a B-Tree for indexing which gets polluted with NULLS in sparse data. Therefore, we set a threshold to route the data into either SQL or MongoDB based on the % of NULL values.

3. Stability: The problem statement mentions type-drift happening occassionally. In an SQL table, Alter commands are highly expensive DDL commands. Therefore, we route fields which alter considerably to the MongoDB after counting the frequency of changes.

4. Length Variance: SQL optimizes data storage and indexing based on fixed lengths of the data stored. A field with extreme variance in length forces the SQL engine to store using out-of-line storage which slows down the operations. Therefore, we calculate the variance for the streaming data using the Welford's Online Algorithm and route to MongoDB for significant variation.

### Uniqueness:

Within the system, data uniqueness is determined by continuously tracking the cardinality of incoming fields. As records are processed, the system maintains a running count of total observations for each field and simultaneously attempts to store the observed values in a specialized data structure that automatically filters out duplicates. To evaluate uniqueness, the system calculates a cardinality ratio by dividing the number of distinct values by the total number of observations. A field is strictly classified as unique only if this ratio is exactly 1.0, meaning every single recorded value for that field was completely distinct without any repetition.

### Value Interpretation:

We handled this using a pre-determined hierarchy for type inference. Each attribute is initially assigned the datatype UNK, and then evaluated sequentially through the following order: bool → int → float → list → string. The datatype that best matches the column’s values is ultimately selected. 

### Mixed Data Handling:

We implemented a system that first attempts to match the incoming value with the previously stored datatype. If the conversion is successful, the value is stored in the same column. If the conversion fails, the system infers a new datatype and triggers an ALTER TABLE request to update the schema accordingly.

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
