# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
import delta
#Nada

# COMMAND ----------

# DBTITLE 1,Fuction
def table_exists(catalog,database,table) :

    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"DATABASE = '{database}' AND tableName = '{table}'")
                .count())
    return count ==1

# COMMAND ----------

# DBTITLE 1,Variaveis
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

# DBTITLE 1,Incremental Load
df_cdc = spark.read.format("csv").option("header", "true").load(f"/Volumes/raw/{database}/cdc")
df_cdc.display()

# COMMAND ----------

# DBTITLE 1,Capturando o ultimo registro



window_spec = Window.partitionBy(f"{id_field}").orderBy(col(f"{timestamp_field}").desc())
df_cdc_unique = df_cdc.withColumn("row_num", row_number().over(window_spec)) \
                .filter("row_num = 1") \
                .drop("row_num")
df_cdc_unique.display()


# COMMAND ----------

# DBTITLE 1,Carregando a Tabela Full Load


bronze = delta.DeltaTable.forName(spark, f"{catalog}.{database}.{table}")
bronze

# COMMAND ----------

# DBTITLE 1,UPSERT
#UPSERT

(bronze.alias("b")
    .merge(df_cdc_unique.alias("d"), f"b.{id_field} = d.{id_field}")
    .whenMatchedDelete(condition= "d.OP = 'D'")
    .whenMatchedUpdateAll(condition= "d.OP = 'U'")
    .whenNotMatchedInsertAll(condition= "d.OP = 'I' OR d.OP = 'U'")
    .execute()
    )
    
