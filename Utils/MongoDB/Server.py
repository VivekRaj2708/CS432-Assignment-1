import pymongo
from Utils.MongoDB.Credentials import USER, PASS, CLUSTER
import re

class Server:
    def __init__(self):
        self.client = pymongo.MongoClient(f"mongodb+srv://{USER}:{PASS}@{CLUSTER}/")

    def get_database(self, name):
        return self.client[name]
    
    def get_collection(self, db_name, collection_name):
        db = self.get_database(db_name)
        return db[collection_name]
    
    def getRecords(
        self,
        db_name,
        collection_name,
        fields=None,
        where=None,
        group_by=None,
        sort=None,
        limit=None
    ):
        collection = self.get_collection(db_name, collection_name)
        where = where or {}
        fields = fields or []

        def _parse_sort(sort_items):
            if not sort_items:
                return []
            result = []
            for item in sort_items:
                if isinstance(item, str):
                    direction = pymongo.DESCENDING if item.startswith("-") else pymongo.ASCENDING
                    key = item[1:] if item[:1] in ["-", "+"] else item
                    result.append((key, direction))
                elif isinstance(item, (list, tuple)) and len(item) == 2:
                    result.append((item[0], item[1]))
            return result

        # Aggregation path (group_by)
        if group_by:
            pipeline = []

            if where:
                pipeline.append({"$match": where})

            # Safe aliases for grouped keys (in case of dotted field names)
            group_alias = {g: g.replace(".", "__") for g in group_by}
            group_id = {group_alias[g]: f"${g}" for g in group_by}
            group_stage = {"_id": group_id}

            # Handle aggregates in fields, currently COUNT(...)
            count_pattern = re.compile(r"^COUNT\s*\(\s*([^)]+)\s*\)$", re.IGNORECASE)
            for f in fields:
                if f in group_by:
                    continue
                m = count_pattern.match(f)
                if m:
                    group_stage[f] = {"$sum": 1}
                else:
                    # keep one representative value if requested
                    group_stage[f] = {"$first": f"${f}"}

            pipeline.append({"$group": group_stage})

            # Project grouped fields back with original names
            project_stage = {"_id": 0}
            for g in group_by:
                project_stage[g] = f"$_id.{group_alias[g]}"
            for f in fields:
                if f not in group_by:
                    project_stage[f] = 1
            pipeline.append({"$project": project_stage})

            sort_spec = _parse_sort(sort)
            if sort_spec:
                pipeline.append({"$sort": dict(sort_spec)})

            if isinstance(limit, int) and limit > 0:
                pipeline.append({"$limit": limit})

            return list(collection.aggregate(pipeline))

        # Simple find path
        projection = {f: 1 for f in fields} if fields else None
        cursor = collection.find(where, projection) if projection is not None else collection.find(where)

        sort_spec = _parse_sort(sort)
        if sort_spec:
            cursor = cursor.sort(sort_spec)

        if isinstance(limit, int) and limit > 0:
            cursor = cursor.limit(limit)

        return list(cursor)
    
    def addRecords(self, db_name, collection_name, data):
        collection = self.get_collection(db_name, collection_name)

        if data is None:
            raise ValueError("data cannot be None")

        if isinstance(data, dict):
            result = collection.insert_one(data)
            return {
                "inserted_count": 1,
                "inserted_id": str(result.inserted_id)
            }

        if isinstance(data, list):
            if not data:
                return {
                    "inserted_count": 0,
                    "inserted_ids": []
                }
            result = collection.insert_many(data)
            return {
                "inserted_count": len(result.inserted_ids),
                "inserted_ids": [str(_id) for _id in result.inserted_ids]
            }

        raise TypeError("data must be a dict (single record) or list (multiple records)")

    def changeRecords(
        self,
        db_name,
        collection_name,
        where,
        data,
        upsert=False,
        multi=True
    ):
        collection = self.get_collection(db_name, collection_name)

        if where is None or not isinstance(where, dict):
            raise ValueError("where must be a dict")
        if data is None or not isinstance(data, dict) or not data:
            raise ValueError("data must be a non-empty dict")

        update_doc = {"$set": data}

        if multi:
            result = collection.update_many(where, update_doc, upsert=upsert)
            return {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }

        result = collection.update_one(where, update_doc, upsert=upsert)
        return {
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "upserted_id": str(result.upserted_id) if result.upserted_id else None
        }
    
    def removeRecords(
        self,
        db_name,
        collection_name,
        where,
        multi=True
    ):
        collection = self.get_collection(db_name, collection_name)

        if where is None or not isinstance(where, dict) or not where:
            raise ValueError("where must be a non-empty dict")

        if multi:
            result = collection.delete_many(where)
            return {
                "deleted_count": result.deleted_count
            }

        result = collection.delete_one(where)
        return {
            "deleted_count": result.deleted_count
        }


    def close(self):
        self.client.close()

        