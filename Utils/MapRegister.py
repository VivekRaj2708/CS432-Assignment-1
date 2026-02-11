import os
from Utils.Log import logger
from Utils.Resolve import Metadata
from tabulate import tabulate
from pickle import dumps, loads


class MapRegister:
    def __init__(self):
        self.map = {}
        self.super = {}

    def ResolveRequest(self, request):
        for key in request:
            # print(f"Resolving key: {key} with value: {request[key]}")
            if isinstance(request[key], dict):
                # print(f"Key {key} is a nested dict; delegating to super MapRegister")
                if key not in self.super:
                    self.map[key] = MapRegister()
                    logger.info(f"Created new MapRegister for key: {key}")
                self.map[key].ResolveRequest(request[key])
                # print(f"Finished resolving nested dict for key: {key}")
                # print(f"Current state of super[{key}]: {self.map[key]}")
            elif key in self.map:
                self.map[key].resolveValue(request[key])
            else:
                self.map[key] = Metadata(type_="UNK")
                self.map[key].resolveValue(request[key])
    
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
        
    