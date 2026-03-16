import json
from Utils.Classify import FieldClassifier


def sql_schema_maker(schema,filename="sql_schema.log"):
    with open(filename,"w") as f:
        for table,table_def in schema.items():
            cols= []
            constraints= table_def.get("table@constraints",[])

            for col,spec in table_def.items():
                if col== "table@constraints":
                    continue

                col_type= spec[0]
                rest= []

                for s in spec[1:]:
                    if not s.startswith("FK"):
                        rest.append(s)

                col_def= f"{col} {col_type}"
                if rest:
                    col_def+= " " + " ".join(rest)

                cols.append(col_def)

            all_defs= cols+constraints
            query= f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(all_defs)});"
            f.write(query+ "\n")


def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, dict)):
        return f"'{json.dumps(v)}'"
    escaped = str(v).replace("'", "''")
    return f"'{escaped}'"


def create_table_query(table_name, columns):
    col_defs = []
    for col in columns:
        if col == "table_autogen_id":
            col_defs.append("table_autogen_id INTEGER PRIMARY KEY")
        elif col == "sys_ingested_at":
            col_defs.append("sys_ingested_at REAL NOT NULL UNIQUE") 
        elif col == "t_stamp":
            col_defs.append("t_stamp REAL")
        else:
            col_defs.append(f"{col} TEXT")
    cols_sql = ", ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});"


def alter_table_add_column_query(table_name, column):
    if column == "sys_ingested_at":
        return f"ALTER TABLE {table_name} ADD COLUMN sys_ingested_at REAL NOT NULL UNIQUE;"
    if column == "t_stamp":
        return f"ALTER TABLE {table_name} ADD COLUMN t_stamp REAL;"
    if column == "table_autogen_id":
        return f"ALTER TABLE {table_name} ADD COLUMN table_autogen_id INTEGER PRIMARY KEY;"
    return f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"


def insert_query(table, columns, values):
    cols = ", ".join(columns)
    vals = ", ".join(sql_value(v) for v in values)
    return f"INSERT INTO {table} ({cols}) VALUES ({vals});"


def sql_from_queue(
    updates,
    filename="sql_queries.log",
    classifier: FieldClassifier = None,
):
    table_columns = {}

    with open(filename, "w") as f:
        for update in updates:
            if update.get("type") != "INSERT":
                continue

            table   = update["table_name"]
            columns = list(update["columns"])
            values  = list(update["values"])

            sys_ingested_at = update.get("sys_ingested_at")
            t_stamp         = update.get("t_stamp")

            if classifier is not None:
                filtered_cols, filtered_vals = [], []
                for col, val in zip(columns, values):
                    if classifier.get_classification(col) == "sql":
                        filtered_cols.append(col)
                        filtered_vals.append(val)
                columns, values = filtered_cols, filtered_vals

            if sys_ingested_at is not None and "sys_ingested_at" not in columns:
                columns.append("sys_ingested_at")
                values.append(sys_ingested_at)
            if t_stamp is not None and "t_stamp" not in columns:
                columns.append("t_stamp")
                values.append(t_stamp)

            if not columns:
                continue

            if table not in table_columns:
                f.write(create_table_query(table, columns) + "\n")
                table_columns[table] = set(columns)
            else:
                for col in columns:
                    if col not in table_columns[table]:
                        f.write(alter_table_add_column_query(table, col) + "\n")
                        table_columns[table].add(col)

            f.write(insert_query(table, columns, values) + "\n")
