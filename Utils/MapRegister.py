import os
from Utils.Log import logger
from Utils.Resolve import Metadata
from tabulate import tabulate
from pickle import dumps, loads
from collections import deque


class MapRegister:
    def __init__(self):
        self.map = {"table_autogen_id": Metadata(type_="int", auto=True)}

    
    def __getitem__(self, key):
        return self.map[key]
    
    def __contains__(self, item):
        return item in self.map
    
    def __iter__(self):
        return iter(self.map)

    def resolve_nested_list(self, key, items, updateOrder: deque=None):
        for item in items:
            if isinstance(item, dict):
                if key not in self.map or not isinstance(self.map[key], MapRegister):
                    self.map[key] = MapRegister()
                    r = deque()
                    self.map[key].ResolveRequest(item, r)
                    updateOrder.append({
                        "type": "CREATE",
                        "table_name": key,
                        "table_map": self.map[key]
                    })
                    while r:
                        updateOrder.append(r.popleft())
                    logger.info(f"Created new MapRegister for key: {key}")
                else:
                    self.map[key].ResolveRequest(item, updateOrder=updateOrder)
            elif isinstance(item, list):
                self.resolve_nested_list(key, item, updateOrder=updateOrder)

    def ResolveRequest(self, request, updateOrder=None):
        table_autogen_id = self.map['table_autogen_id'].resolveValue(queue=updateOrder) # Increment the auto ID for each request
        for key in request:
            value = request[key]
            if isinstance(value, dict):
                if key not in self.map or not isinstance(self.map[key], MapRegister):
                    self.map[key] = MapRegister()
                    r = deque()
                    self.map[key].ResolveRequest(value, r)
                    updateOrder.append({
                        "type": "CREATE",
                        "table_name": key,
                        "table_map": self.map[key]
                    })
                    while r:
                        updateOrder.append(r.popleft())
                    logger.info(f"Created new MapRegister for key: {key}")
                else:
                    self.map[key].ResolveRequest(value, updateOrder=updateOrder)
            elif isinstance(value, list):
                if any(isinstance(item, (dict, list)) for item in value):
                    self.resolve_nested_list(key, value, updateOrder=updateOrder)
                elif key in self.map:
                    self.map[key].resolveValue(value)
                else:
                    self.map[key] = Metadata(type_="UNK")
                    self.map[key].resolveValue(value)
            elif key in self.map:
                self.map[key].resolveValue(value)
            else:
                self.map[key] = Metadata(type_="UNK")
                self.map[key].resolveValue(value)
    
    def __repr__(self):
        # print("MapRegister __repr__ called; preparing tabulated output")
        # print(f"Current map contents: {self.map}")
        return tabulate([[k, v] for k, v in self.map.items()], headers=["Key", "Metadata"], tablefmt="grid")

    def Save(self, filename=None):
        if filename is None:
            logger.warning("No filename provided for Save; using default 'map_register.pkl'")
            filename = "map_register.pkl"
        with open(filename, "wb") as f:
            f.write(dumps(self.map))
        logger.info(f"MapRegister saved to {filename}")
    
    def Load(self, filename=None):
        if filename is None:
            logger.warning("No filename provided for Load; using default 'map_register.pkl'")
            filename = "map_register.pkl"
        if not os.path.exists(filename):
            logger.error(f"File {filename} does not exist; cannot load MapRegister")
            return
        with open(filename, "rb") as f:
            self.map = loads(f.read())
        logger.info(f"MapRegister loaded from {filename}")
        logger.info(f"Loaded Data: \n{self}")
        
    