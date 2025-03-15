# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
import delta


# COMMAND ----------

# DBTITLE 1,Fuction
def table_exists(catalog,database,table) :

    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"DATABASE = '{database}' AND tableName = '{table}'")
                .count())
    return count ==1

# COMMAND ----------

# DBTITLE 1,Variaveis
"""catalog = "bronze"
database = "sys_reclamacao"
table = "reclamacao"
id_field = "Nome"
timestamp_field ="modified_date""""


catalog = "bronze"
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")

# COMMAND ----------

# DBTITLE 1,Full Load
if not table_exists(catalog,database,table):

    print("Table does not exist")

    df_full = spark.read.format("csv").option("header", "true").load(f"/Volumes/raw/{database}/full_load")    
    df_full.coalesce(1).write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{database}.{table}")
else :
    print("Table already exists")


# COMMAND ----------

df_cdc = spark.read.format("csv").option("header", "true").load(f"/Volumes/raw/{database}/cdc") 
schema = df_cdc.schema

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark, f"{catalog}.{database}.{table}")

# COMMAND ----------

# DBTITLE 1,Incremental Load
df_cdc = (spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .schema(schema)\
    .load(f"/Volumes/raw/{database}/cdc/{table}/")
)
        



# COMMAND ----------

def upsert(df, deltatable):
    
    window_spec = Window.partitionBy(f"{id_field}").orderBy(col(f"{timestamp_field}").desc())
    df_cdc_unique = df_cdc.withColumn("row_num", row_number().over(window_spec)) \
                .filter("row_num = 1") \
                .drop("row_num")

    (deltatable.alias("b")
    .merge(df_cdc_unique.alias("d"), f"b.{id_field} = d.{id_field}")
    .whenMatchedDelete(condition= "d.OP = 'D'")
    .whenMatchedUpdateAll(condition= "d.OP = 'U'")
    .whenNotMatchedInsertAll(condition= "d.OP = 'I' OR d.OP = 'U'")
    .execute()
    )

# COMMAND ----------

stream = (df_cdc.writeStream\
            .option("checkpointLocation",f"/Volumes/raw/{database}/cdc/{table}_checkpoint/") \
            .option("header", "true")\
            .option("cloudFiles.maxFilesPerTrigger", "500")
            .foreachBatch(lambda df, batchId: upsert(df,bronze))
            .trigger(availableNow=True).start()
)

# COMMAND ----------

start = stream

# COMMAND ----------

start.stop()
