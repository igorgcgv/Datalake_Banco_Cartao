
import delta
from pyspark.sql import types
import json



def table_exists(spark,catalog,database,table) :

    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"DATABASE = '{database}' AND tableName = '{table}'")
                .count())
    return count ==1




def import_schema(table):

    with open(f"{table}.json", "r") as open_file:
        shcema_json = json.load(open_file)
    schema = types.StructType.fromJson(shcema_json)
    return schema
