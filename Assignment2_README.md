# CS 432 Assignment 2

## Project Structure
```
CS432-Assignment-2/
│
├── T2/
│   ├── client_simulation.py
│   ├── readme.md
│   ├── requirements.txt
│   ├── simulation.py
│   └── uni_schema.json
│
├── Utils/
│   ├── MongoDB/
│   │   ├── Server.py
│   │   └── Exec.py
│   │
│   ├── MySQL/
│   │   ├── crud_debugger.py
│   │   └── query_executor.py
│   │
│   └── Other Files
│       ├── Algo.py
│       ├── BiTemporal.py
│       ├── Classify.py
│       ├── Log.py
│       ├── MapRegister.py
│       ├── Network.py
│       ├── Resolve.py
│       ├── SQL.py
│       ├── schema_maker.py
│       └── test.py
│
├── Assignment2_README.md
├── Assignment2_report.pdf
├── Assignment2_report.md
└──
```

## Setup and Steps to execute the code
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