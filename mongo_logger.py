import json
from Utils.Classify import FieldClassifier




def mongo_value(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    escaped = str(v).replace('"', '\\"')
    return f'"{escaped}"'


def generate_mongo_insert(table, columns, values):
    doc_parts = [f'{col}: {mongo_value(val)}' for col, val in zip(columns, values)]
    return f"db.{table}.insertOne({{{', '.join(doc_parts)}}});"


def mongo_from_queue(
    updates,
    filename="mongo_queries.log",
    classifier: FieldClassifier = None,
):
    with open(filename, "w") as f:
        for update in updates:
            if update.get("type") != "INSERT":
                continue

            table           = update["table_name"]
            columns         = list(update["columns"])
            values          = list(update["values"])
            sys_ingested_at = update.get("sys_ingested_at")
            t_stamp         = update.get("t_stamp")
            original_record = update.get("_original_record", {})

            if classifier is not None:
                filtered_cols, filtered_vals = [], []
                for col, val in zip(columns, values):
                    if classifier.get_classification(col) == "mongodb":
                        filtered_cols.append(col)
                        filtered_vals.append(val)
                columns, values = filtered_cols, filtered_vals

            if original_record:
                for key, val in original_record.items():
                    is_nested = isinstance(val, dict) or (
                        isinstance(val, list)
                        and any(isinstance(i, (dict, list)) for i in val)
                    )
                    if is_nested and key not in columns:
                        columns.append(key)
                        values.append(val)

            if sys_ingested_at is not None and "sys_ingested_at" not in columns:
                columns.append("sys_ingested_at")
                values.append(sys_ingested_at)
            if t_stamp is not None and "t_stamp" not in columns:
                columns.append("t_stamp")
                values.append(t_stamp)

            if not columns:
                continue

            f.write(generate_mongo_insert(table, columns, values) + "\n")
