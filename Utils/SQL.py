from tabulate import tabulate
from pickle import dump, load

class Metadata:
    
    def __init__(self, type_, auto = False):
        assert type_ in ["int", "string", "float", "bool"], "Unsupported type"
        assert not auto or type_ == "int", "AUTO can only be set for int type"
        
        self.type = type_
        self.auto = auto
        self.current_value = 0 if auto else None
        
    
    def __repr__(self):
        return f"Metadata(type={self.type}" + (", AUTO" if self.auto else "") + ")"

    def acceptValue(self, value=None):
        assert value is None and not self.auto, "Value must be provided unless AUTO is set"
        if self.auto:
            self.current_value += 1
            return self.current_value
        return value
        
class Table:
    
    def __init__(self, name):
        self.name = name
        self.auto_gen_id = 0
        self.columns = {"ID": {'metadata': Metadata("int", auto=True)}}
    
    def __repr__(self):
        headers = ["Column Name", "Type"]
        rows = []
        for x in self.columns:
            if 'table' in self.columns[x]:
                source = self.columns[x]['table']
            else:
                source = self.columns[x]['metadata']
            rows.append([x, source])
        return f"Table(name={self.name})\n" + tabulate(rows, headers, tablefmt="grid")

        
    def addColumn(self, name, metadata = None, table = None):
        assert name not in self.columns, f"Column {name} already exists"
        assert table is not None or metadata is not None, "Must provide either metadata or table"
        if table is not None:
            assert metadata is None, "Cannot provide both metadata and table"
            assert isinstance(table, Table), "table must be an instance of Table"
            self.columns[name] = {'table': table}
        else:
            assert metadata is not None, "Must provide either metadata or table"
            assert isinstance(metadata, Metadata), "metadata must be an instance of Metadata"
            self.columns[name] = {'metadata': metadata}
    
    def acceptMapping(self, mapping: dict) -> str:
        assert isinstance(mapping, dict), "Mapping must be a dictionary"
        
        genMappings = []
        genCommands = []
        for col in self.columns:
            if isinstance(self.columns[col].get('table'), Table):
                genCommands.append(self.columns[col]['table'].acceptMapping(mapping[col]))
            
            
        # Further validation can be added here
        return genCommands
    
    def DDLQuery(self):
        query = f"CREATE TABLE {self.name} (\n"
        col_defs = []
        for col_name, col_info in self.columns.items():
            if 'metadata' in col_info:
                col_type = col_info['metadata'].type
                col_defs.append(f"  {col_name} {col_type}")
            elif 'table' in col_info:
                ref_table = col_info['table'].name
                col_defs.append(f"  {col_name} INT, -- Foreign key to {ref_table}\n  FOREIGN KEY ({col_name}) REFERENCES {ref_table}(id)")
        query += ",\n".join(col_defs)
        query += "\n);"
        return query

    def AddDataQuery(self, data):
        cols = ", ".join(data.keys())
        vals = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        query = f"INSERT INTO {self.name} ({cols}) VALUES ({vals});"
        return query

    def SaveTable(self):
        with open(f"{self.name}_table.pkl", "wb") as f:
            dump(self, f)
        pass
    
    @staticmethod
    def LoadTable(name):
        with open(f"{name}_table.pkl", "rb") as f:
            table = load(f)
        return table

## Tests
if __name__ == "__main__":
    user_table = Table("User")
    user_table.addColumn("name", metadata=Metadata("string"))
    
    order_table = Table("Order")
    order_table.addColumn("user", table=user_table)
    
    print(order_table)
    # print(user_table.DDLQuery())
    # print(order_table.DDLQuery())
    
    # data_entry = {"id": 1, "name": "Alice" }
    # print(user_table.AddDataQuery(data_entry))    
    # data_entry_order = {"order_id": 101, "user": 1 }
    # print(order_table.AddDataQuery(data_entry_order))
    # print(user_table.DDLQuery())