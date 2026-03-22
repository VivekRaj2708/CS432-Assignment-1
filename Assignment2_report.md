# CS 432 Assignment 2 Report

# Task: Autonomous Normalization & CRUD Engine Hybrid Database Framework

## Github Repository Link
[Link to Repository](https://github.com/VivekRaj2708/CS432-Assignment-1)

## Team Members: 
- Harinarayan J (23110128) 
- Nupoor Assudani (23110224)  
- Pavitr Chandwani (23110241)  
- Pranav Thakkar (23110253) 
- Vivek Raj (23110362)

## Methodology
Our system follows a streaming, metadata-driven methodology to automatically infer database structure and generate executable queries without prior schema knowledge. Incoming data is simulated using a FastAPI-based SSE stream, where records are accompanied by metadata specifying global and unique keys. These records are processed incrementally using a dual-buffer strategy (400-record and 1000-record windows) to detect functional dependencies and relational patterns.

The system identifies entities by analyzing dependencies between unique fields and other attributes, while relationships such as foreign keys and many-to-many mappings are inferred through set-based comparisons and detection of nested list structures.

Finally, the inferred schema and metadata are used to generate SQL operations corresponding to CRUD events. These operations are logged and later executed using a MySQL execution engine, while parallel MongoDB handling is supported through an asynchronous query processor.

## Functionality
The system provides a complete pipeline for intelligent data ingestion, schema inference, query generation, and execution across both relational and document-based databases. It accepts streaming JSON records and dynamically classifies fields based on metadata and observed data patterns. The SchemaInfere engine processes incoming records to detect entities, functional dependencies, foreign keys, and many-to-many relationships, constructing normalized relational tables and junction tables where necessary.

The system supports all CRUD operations through event-driven processing. Insert operations are generated when a new primary key is observed, while updates are triggered for repeated keys. All generated operations are stored in structured logs and JSON formats, enabling traceability and execution.

For execution, the system integrates two engines: a MySQLQueryExecutor for executing SQL queries and logging results, and an asynchronous MongoDB execution engine that processes document-based queries using worker queues.

## Approach
The schema inference engine operates independently of the data source, allowing it to process any streaming JSON input without predefined schemas. It employs rule-based heuristics such as functional dependency detection, subset checking for foreign keys, and list-based detection for many-to-many relationships.

To handle evolving data, the system incorporates a schema snapshot mechanism that compares current and previous states to detect structural changes.

The execution layer is divided into relational and document-based components. SQL queries are generated and executed using a dedicated MySQL executor, while MongoDB operations are processed asynchronously using a queue-based worker system.

## Workflow of Codebase:
User input (column names with certain identifiers) --> Streaming of Data --> Ingestion and Table Creation --> Data Insertion with classification between MySQL and MongoDB --> CRUD queries executer and generator --> JSON output

## Answers to the asked questions
### Normalization Strategy
The system performs automatic normalization by analyzing functional dependencies within streaming data. Using batches of records, it checks whether a unique field consistently determines other attributes, thereby identifying entity boundaries.

### Table Creation Logic
Tables are created based on detected unique fields, which serve as primary keys. Each unique field corresponds to one entity table, and its dependent attributes are included as columns.
Foreign keys are identified by checking subset relationships between attribute values and unique key domains. When such relationships are detected, foreign key constraints are added to maintain referential integrity. Additionally, schema evolution is supported by dynamically generating CREATE and ALTER TABLE statements whenever new entities or attributes are discovered.

### MongoDB Design Strategy
The MongoDB strategy is based on analyzing the structure and behavior of incoming data. Nested fields represented as lists are treated as candidates for embedding or separation. If the nested data is tightly coupled and relatively small, it can be embedded within a parent document. However, if it grows large or represents shared relationships (such as many-to-many), it is stored as a separate collection with references.

### Metadata System
The metadata system is central to the architecture and stores information about unique fields, global keys, inferred entities, functional dependencies, foreign keys, and table mappings. It also maintains runtime information such as schema snapshots and previously seen primary keys.

This metadata is used at multiple stages: during schema inference to determine table structures, during query generation to map fields to storage locations, and during execution to ensure consistency.

### CRUD Query Generation
CRUD operations are generated dynamically based on incoming events. For insert operations, the system checks whether a primary key has been seen before; if not, it generates an INSERT query, otherwise an UPDATE query is created. Read operations use provided column hints to generate SELECT queries with appropriate filtering conditions.

Update operations modify only the specified fields, while delete operations generate DELETE queries with conditions based on primary keys. These operations are converted into executable SQL strings and stored in structured JSON format, enabling seamless execution by the MySQL engine and parallel handling in MongoDB.

### Performance Considerations
The system is designed to handle streaming data efficiently using batching and incremental processing. By processing records in buffers of 400 and 1000, it balances accuracy of inference with computational overhead. The use of asynchronous workers in the MongoDB execution engine allows parallel processing of queries, improving throughput. Additionally, maintaining a record of seen primary keys reduces redundant insert operations and minimizes database overhead.


## Tools & External Sources used
- Official Documentation of MySQL and MongoDB
- [Link for Normalization Strategy](https://terminalnotes.com/database-normalization-in-sql-explained-1nf-2nf-3nf-with-examples/)
- Stack Overflow posts

## Contribution
- Harinarayan J: System and Workflow Ideation
- Nupoor Assudani: Data Streaming and Client Simulation
- Pavitr Chandwani: CRUD queries and engine, MySQL connection
- Pranav Thakkar: Schema Inference and Normalization
- Vivek Raj: MongoDB connection and respective CRUD queries
- Everyone contribued equally to the report