# Databricks notebook source
def import_query(path):
    with open(path, 'r') as f:
        return f.read()
    
table= dbutils.widgets.get("table") 
query = import_query(f"{table}.sql")



(spark.sql(query)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"silver.cartoes.{table}"))
