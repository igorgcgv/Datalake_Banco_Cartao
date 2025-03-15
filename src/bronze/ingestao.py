# Databricks notebook source
# DBTITLE 1,Imports

import delta
import sys

sys.path.insert(0, '../lib/')

import utils


# COMMAND ----------

# DBTITLE 1,Fuction


# COMMAND ----------

# DBTITLE 1,Variaveis
catalog = "bronze"
database = "sys_transacao"
table = "transacao"
id_field = "Nome"
timestamp_field ="modified_date"

"""
catalog = "bronze"
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")"""

# COMMAND ----------

# DBTITLE 1,Full Load
if not utils.table_exists(spark,catalog,database,table):

    print("Table does not exist")

    df_full = spark.read.format("csv").option("header", "true").load(f"/Volumes/raw/{database}/full_load")    
    df_full.coalesce(1).write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{database}.{table}")
else :
    print("Table already exists")


# COMMAND ----------


schema = utils.import_schema(table)

# COMMAND ----------

df_cdc = spark.read.format("csv").option("header", "true").load(f"/Volumes/raw/{database}/cdc") 


# COMMAND ----------

schema.json()

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
