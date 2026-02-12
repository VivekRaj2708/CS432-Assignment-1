# CS432 Assignment 1

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](./tests/)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)

**Dynamic Schema Inference & Real-Time Event Processing System**

A Python-based system for streaming Server-Sent Events (SSE), automatically inferring data schemas, tracking type transitions, and generating SQL-like operations for database synchronization. Built for CS432 coursework demonstrating advanced data processing patterns.

**[View Assignment Report](Assignment_Report.md)**

---

## Features

- **Real-Time SSE Streaming**: Asynchronous event ingestion with automatic reconnection and queue management
- **Dynamic Schema Inference**: Automatically detects and adapts to changing data structures
- **Intelligent Type Resolution**: Handles type transitions with configurable rules (int → float → list<T> → str)
- **SQL Operation Tracking**: Generates CREATE, ALTER, and INSERT operations for schema synchronization
- **Nested Structure Support**: Recursively processes nested dictionaries and lists with hierarchical table naming
- **Persistent Schema Storage**: Pickle-based serialization for schema preservation across sessions
- **Comprehensive Testing**: pytest suite covering type transitions, network streaming, and edge cases

---

## Project Structure

```
CS432-Assignment-1/
├── Utils/               # Core utility modules
│   ├── Network.py      # SSE streaming and HTTP client
│   ├── Resolve.py      # Type resolution and metadata management
│   ├── MapRegister.py  # Schema registry and SQL operation generation
│   ├── SQL.py          # SQL query generation utilities
│   ├── Log.py          # Logging configuration
│   └── Algo.py         # Algorithm utilities
├── tests/              # Test suite
│   ├── test_network.py    # Network streaming tests
│   └── test_resolve.py    # Type resolution tests
├── Doc/                # Documentation
│   ├── Network.md      # Network module documentation
│   └── Resolver.md     # Resolve & MapRegister documentation
├── Power/              # Power systems analysis notebooks
│   └── T2b.ipynb      # Power simulation notebook
├── T2/                 # Simulation module
│   └── simulation.py  # Simulation utilities
├── Runner.py           # Main application entry point
├── server.ps1/sh       # Development server scripts
├── setup.ps1/sh        # Environment setup scripts
├── requirements.txt    # Python dependencies
└── Assignment_Report.md # Detailed assignment report
```

---

## Installation

### Prerequisites
- Python 3.13 or higher
- pip package manager
- Virtual environment (recommended)

### Setup

**Windows (PowerShell):**
```powershell
# Run the setup script
.\setup.ps1

# Or manually:
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux/macOS:**
```bash
# Run the setup script
chmod +x setup.sh
./setup.sh

# Or manually:
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Usage

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

This will:
1. Connect to the SSE endpoint
2. Stream records (configurable count)
3. Infer and update the schema dynamically
4. Generate SQL operations for each record
5. Save the final schema to `final_map_register.pkl`
6. Output schema to `Map.log`

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_resolve.py

# Run with coverage
pytest --cov=Utils
```

**Test Coverage:**
- Type resolution and transitions
- Network streaming with backpressure
- Nested structure handling
- Schema persistence
- Edge cases (empty lists, mixed types, etc.)

---

## Documentation

Comprehensive documentation is available in the `Doc/` directory:

- **[Network Module](Doc/Network.md)** - SSE streaming, HTTP clients, queue management
- **[Resolver Module](Doc/Resolver.md)** - Type resolution, schema inference, SQL operations

### Key Concepts

**Type Transition Rules:**
- `int` → `float`, `list<int>`, `list<float>`, `str`
- `float` → `list<float>`, `str`
- `bool` → `int`, `float`, `list<int>`, `list<float>`, `str`
- `str` → `list<str>`

**SQL Operations:**
- `CREATE`: New table for nested dictionary
- `ALTER`: New column or type change
- `INSERT`: Row insertion with all column values

---

## Configuration

### Environment Variables

No environment variables required. Configuration is handled via:

- `Runner.py` parameters (count, max_queue_size)
- `MapRegister` table naming
- Server endpoint URLs in `Network.py`

### Logging

Logs are written to `logs.log` by default. Configure logging in `Utils/Log.py`.

---

## Performance Considerations

- **Queue Management**: Use `max_queue_size` to prevent memory overflow
- **Type Inference**: Lazy evaluation on first value
- **Nested Structures**: O(n) registries where n = nesting depth
- **Persistence**: Pickle serialization includes full schema tree
- **Backpressure**: Automatic pause when queue threshold reached

---

## Author

1. **Vivek Raj** ([@VivekRaj2005](https://github.com/VivekRaj2005))
- **Roll Number:** 23110362
- **Email:** [vivek.raj@iitgn.ac.in](mailto:vivek.raj@iitgn.ac.in )
