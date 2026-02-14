# CS 432 Assignment 1 Report

# Task: Adaptive Ingestion & Hybrid Backend Placement

## Team Members: 
- Harinarayan J (23110128) 
- Nupoor Assudani (23110224)  
- Pavitr Chandwani (23110241)  
- Pranav Thakkar (23110253) 
- Vivek Raj (23110362)

## Project Overview:
This project implements an autonomous data ingestion system that dynamically decides whether incoming JSON data is better suited for MySQL or MongoDB.

We have implemented certain heuristics such as frequency, which will be discussed ahead and on basis of that will automaticaaly decide the data storage.

## Our Approach:
JSON Stream (with logging) --> Cleaning of the data --> Mapping of Data --> Datatype handling --> Decision of Database based on heuristics --> Ingestion

### Workflow of files:
The module `Network.py` is an asynchronous GET wrapper that returns the parsed JSON stream or raises errors on HTTP, as we are using HTTPX to get the stream records asynchronously using the `stream_sse_records` function. It cleans the data and also attaches the required timestamp with as asked in the assignment and in the end it rpovides a queue with the following record. It also logs any errors in a file (if any), and also provides control tot he user to set the maximum size of queue to handle unnecesary pressure.

The module `Resolve.py` implements a dynamic metadata and type-resolution system for ingestion. The **Metadata** class performs automatic type inference, and safe value conversions as new records are processed. Initially, an 'UNK' datatype is assigned and then it's solved using a proritized inference strategy. This syste supports types like **int**,**float**,**bool**,**str**, as well as subtype tracking like **list<int>**. The module also supports auto-increment feature. This mainly handles the ingestion part for the SQL database.

The module `MapRegister.py` implements a mapping engine that transforms nested jSOn records into normalized relational representation. It maintains a column metadata using the reviously defined datatype inference system. For every record, the system recursively processes nested objects and lists, automatically creating tables and establishing keys and identifiers. It generates a queue of database operations like **CREATE**,**ALTER**,**INSERT**, etc. The schema state can be serialized and restored using pickle-based persistance allowing the pieline to maintain continuity. 

The module `Log.py` cnfigures a centralized logging system for the pipeline. A root logger is initialized with an INFO severity level to write timestamped entries. The logging inclues the log level, timestamp, and desciptive message helpful in closely monitoring the system and effective debugging if need be. 

### Heuristics we decided on:

### Workflow Diagram:

### Conclusive Workflow:

### Individual Documentation:

## Project Structure:

## Setup and Starting of system:

