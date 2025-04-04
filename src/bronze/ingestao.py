# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
import delta
import sys

sys.path.insert(0, '../lib/')

import utils
import ingestors

# COMMAND ----------

# DBTITLE 1,Variaveis

# Usar como exemplo

"""catalog = "bronze"
database = "sys_cadastro"
table = "Sys_Cadastro"
id_field = "Nome"
timestamp_field ="modified_date"
checkpoint_location = f"/Volumes/raw/{database}/cdc/{table}_checkpoint/"""


catalog = "bronze"
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")


# COMMAND ----------

# DBTITLE 1,Full Load
if not utils.table_exists(spark,catalog,database,table):

    print("Table does not exist")

    #dbutils.fs.rm(checkpoint_location,True)

    ingest_full_load = ingestors.ingestor(spark=spark,
                                            catalog=catalog,
                                            database=database,
                                            table=table,
                                            data_format="csv",
                                            id_field=id_field,
                                            timestamp_field=timestamp_field)

    
    ingest_full_load.execute(f"/Volumes/raw/{database}/full_load/{table}/")

else :
    print("Table already exists")


# COMMAND ----------

# DBTITLE 1,Cdc Load
ingest_cdc = ingestors.ingestorCDC(spark=spark,
                                    catalog=catalog,
                                    database=database,
                                    table=table,
                                    data_format="csv",
                                    id_field=id_field,
                                    timestamp_field=timestamp_field)

stream = ingest_cdc.execute(f"/Volumes/raw/{database}/cdc/{table}")
