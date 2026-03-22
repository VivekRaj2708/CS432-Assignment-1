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



def mongo_schema_maker(schema, filename="mongo_schema.log"):       #not a lot as mongo is not realtional database
    #just creating a collection
    with open(filename,"w") as f:
        for table in schema:
            f.write(f'db.createCollection("{table}");\n')