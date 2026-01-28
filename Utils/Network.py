import json
import subprocess

def fetch_data():
    result = subprocess.run(['curl', 'http://127.0.0.1:8000/'], capture_output=True, text=True)
    if result.returncode == 0:
        return json.loads(result.stdout)