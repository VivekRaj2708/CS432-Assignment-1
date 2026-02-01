import requests
import time

session = requests.Session()
session.headers.update({
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache'
})

def fetch_data():
    bust = int(time.time())
    response = requests.get(f"http://127.0.0.1:8000/record/10000?_={bust}", headers={'Cache-Control': 'no-cache'})
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()