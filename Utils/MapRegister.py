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
                        a = r.popleft()
                        a["table_name"] = key
                        updateOrder.append(a)
                    logger.info(f"Created new MapRegister for key: {key}")
                else:
                    self.map[key].ResolveRequest(item, updateOrder=updateOrder)
            elif isinstance(item, list):
                self.resolve_nested_list(key, item, updateOrder=updateOrder)

    def ResolveRequest(self, request, updateOrder=None):
        table_autogen_id = self.map['table_autogen_id'].resolveValue(queue=updateOrder) # Increment the auto ID for each request
        
        # Collect resolved values for INSERT
        insert_columns = []
        insert_values = []
        
        for key in request:
            value = request[key]
            if isinstance(value, dict):
                if key not in self.map or not isinstance(self.map[key], MapRegister):
                    self.map[key] = MapRegister()
                    r = deque()
                    child_id = self.map[key].ResolveRequest(value, r)
                    updateOrder.append({
                        "type": "CREATE",
                        "table_name": key,
                        "table_map": self.map[key]
                    })
                    while r:
                        a = r.popleft()
                        a["table_name"] = key
                        updateOrder.append(a)
                    logger.info(f"Created new MapRegister for key: {key}")
                    insert_columns.append(key)
                    insert_values.append(child_id)
                else:
                    child_id = self.map[key].ResolveRequest(value, updateOrder=updateOrder)
                    insert_columns.append(key)
                    insert_values.append(child_id)
            elif isinstance(value, list):
                if any(isinstance(item, (dict, list)) for item in value):
                    self.resolve_nested_list(key, value, updateOrder=updateOrder)
                elif key in self.map:
                    resolved_val = self.map[key].resolveValue(value, queue=updateOrder, column_name=key)
                    insert_columns.append(key)
                    insert_values.append(resolved_val)
                else:
                    self.map[key] = Metadata(type_="UNK")
                    resolved_val = self.map[key].resolveValue(value, queue=updateOrder, column_name=key)
                    if updateOrder is not None:
                        updateOrder.append({
                            "type": "ALTER",
                            "column_name": key,
                            "old_type": None,
                            "new_type": self.map[key].type if self.map[key].type != "list" else f"list<{self.map[key].subtype.type}>"
                        })
                    insert_columns.append(key)
                    insert_values.append(resolved_val)
            elif key in self.map:
                resolved_val = self.map[key].resolveValue(value, queue=updateOrder, column_name=key)
                insert_columns.append(key)
                insert_values.append(resolved_val)
            else:
                self.map[key] = Metadata(type_="UNK")
                resolved_val = self.map[key].resolveValue(value, queue=updateOrder, column_name=key)
                if updateOrder is not None:
                    updateOrder.append({
                        "type": "ALTER",
                        "column_name": key,
                        "old_type": None,
                        "new_type": self.map[key].type if self.map[key].type != "list" else f"list<{self.map[key].subtype.type}>"
                    })
                insert_columns.append(key)
                insert_values.append(resolved_val)
        
        # Emit INSERT with all columns and values
        if updateOrder is not None:
            updateOrder.append({
                "type": "INSERT",
                "columns": insert_columns,
                "values": insert_values
            })
        
        return table_autogen_id
    
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
        
    