from Utils.Network import stream_sse_records
from Utils.MapRegister import MapRegister
from collections import deque

if __name__ == "__main__":

    data = deque()
    stream_sse_records(100, data)
    
        
