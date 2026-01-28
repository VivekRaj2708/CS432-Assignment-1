#!/bin/sh

source .venv/bin/activate 
uvicorn T2.simulation:app --reload --port 8000 