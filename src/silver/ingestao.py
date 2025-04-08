# Databricks notebook source
"""def import_query(path):
    with open(path, 'r') as f:
        return f.read()
    
table= dbutils.widgets.get("table") 
query = import_query(f"{table}.sql")



(spark.sql(query)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"silver.cartoes.{table}"))"""
sys.path.insert(0, '/Workspace/Repos/igorepufpi@gmail.com/Datalake_Banco_Cartao/src/lib')

import os
import ingestors
import utils
import sys



tablename = "cliente"
idfield = "id_field"
idfield_old = "id_field_old"



"""tablename = dbutils.widgets.get("table")
idfield = dbutils.widgets.get("id_field")
idfield_old = dbutils.widgets.get("id_field_old")"""

catalog = "silver"
schemaname = "sys_cadastro"

# COMMAND ----------

print(utils.__file__)

# COMMAND ----------

remove_checkpoint = False

if not utils.table_exists(spark, "silver", schemaname, tablename):

    print("Criando a tabela", tablename)
    query = utils.import_query(f"{tablename}.sql")
    (spark.sql(query)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"silver.{schemaname}.silver_{tablename}"))
    
    remove_checkpoint = True
