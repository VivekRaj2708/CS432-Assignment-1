import json

def mongo_value(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int,float)):
        return str(v)
    if isinstance(v, (list,dict)):
        return json.dumps(v)
    
    # string
    escaped = str(v).replace('"','\\"')
    return f'"{escaped}"'


def generate_mongo_insert(update):
    collection= update["table_name"]
    columns= update["columns"]
    values= update["values"]

    doc_parts= []
    for col,val in zip(columns,values):
        doc_parts.append(f'{col}: {mongo_value(val)}')

    doc_body= ", ".join(doc_parts)

    return f"db.{collection}.insertOne({{{doc_body}}});"


def mongo_from_queue(updates,filename="mongo_queries.log"):
    with open(filename,"w") as f:
        for update in updates:
            if update.get("type")!= "INSERT":
                continue
            query= generate_mongo_insert(update)
            f.write(query+ "\n")
