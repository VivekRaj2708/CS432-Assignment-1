import json

def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int,float)):
        return str(v)
    if isinstance(v, (list,dict)):
        return f"'{json.dumps(v)}'"
    
    escaped= str(v).replace("'","''")
    return f"'{escaped}'"


def create_table_query(table_name,columns):
    cols= [f"{col} TEXT" for col in columns]
    cols_sql= ", ".join(cols)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});"


def alter_table_add_column_query(table_name,column):
    return f"ALTER TABLE {table_name} ADD COLUMN {column} TEXT;"


def insert_query(update):
    table= update["table_name"]
    columns= update["columns"]
    values= update["values"]

    formatted_values= [sql_value(v) for v in values]

    cols = ", ".join(columns)
    vals = ", ".join(formatted_values)

    return f"INSERT INTO {table} ({cols}) VALUES ({vals});"


def sql_from_queue(updates,filename="queries.log"):
    table_columns= {}  

    with open(filename,"w") as f:

        for update in updates:
            if update.get("type")!="INSERT":
                continue

            table= update["table_name"]
            columns= update["columns"]

            # If table not created
            if table not in table_columns:
                f.write(create_table_query(table, columns)+ "\n")
                table_columns[table]= set(columns)

            else:
                # If any new columns
                existing_cols= table_columns[table]
                for col in columns:
                    if col not in existing_cols:
                        f.write(alter_table_add_column_query(table, col)+ "\n")
                        existing_cols.add(col)

            # inserting new query
            f.write(insert_query(update)+ "\n")
