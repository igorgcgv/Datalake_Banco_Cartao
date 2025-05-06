# Databricks notebook source
import sys

sys.path.insert(0, '../lib/')
import os
import ingestors
import utils


def import_query(path):
    with open(path, 'r') as f:
        return f.read()
"""    
table= dbutils.widgets.get("table") 
query = import_query(f"{table}.sql")



(spark.sql(query)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"silver.cartoes.{table}"))"""








table = "cliente"
idfield = "id_field"
idfield_old = "id_field_old"



"""tablename = dbutils.widgets.get("table")
idfield = dbutils.widgets.get("id_field")
idfield_old = dbutils.widgets.get("id_field_old")"""

catalog = "silver"
database = "sys_cadastro"

# COMMAND ----------

print(utils.__file__)

# COMMAND ----------

remove_checkpoint = False

if not utils.table_exists(spark, "silver", database, table):

    print("Criando a tabela", table)
    query = import_query(f"{table}.sql")
    (spark.sql(query)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"silver.{database}.silver_{table}"))
    
    remove_checkpoint = True

# COMMAND ----------

print("Iniciando CDF...")

ingest = ingestors.ingestorCDF(spark=spark,
                               catalog=catalog,
                               database=database,
                               table=table,
                               id_field=idfield,
                               idfield_old=idfield_old)

if remove_checkpoint:
    dbutils.fs.rm(ingest.checkpoint_location, True)

stream = ingest.execute()
print("Ok.")
