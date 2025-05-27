

import delta
import utils
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

class ingestor:

    def __init__(self, spark, catalog, database, table, data_format, id_field, timestamp_field):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table
        self.format = data_format
        self.id_field = id_field
        self.timestamp_field = timestamp_field        
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(self.table)

    def load(self, path):
        self.df = self.spark.read.format(self.format).schema(self.data_schema).option("header", "true").load(path)
        return self.df

    def save(self, df):
        (df.coalesce(1)
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")  # Habilita CDF
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.database}.{self.table}"))
        return True

    def execute(self, path):
        df = self.load(path)
        return self.save(df)


class ingestorCDC(ingestor):

    def __init__(self, spark, catalog, database, table, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, database, table, data_format, id_field, timestamp_field)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self):
        table = f"{self.catalog}.{self.database}.{self.table}"
        self.deltatable = delta.DeltaTable.forName(self.spark, table)

    def upsert(self, df):
        window_spec = Window.partitionBy(self.id_field).orderBy(col(self.timestamp_field).desc())

        df_cdc_unique = df.withColumn("row_num", row_number().over(window_spec)) \
            .filter("row_num = 1") \
            .drop("row_num")

        (self.deltatable
            .alias("b")
            .merge(df_cdc_unique.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
            .whenMatchedDelete("d.OP = 'D'")
            .whenMatchedUpdateAll("d.OP = 'U'")
            .whenNotMatchedInsertAll("d.OP = 'I' OR d.OP = 'U'")
            .execute()
        )

    def load(self, path):
        df_cdc = (self.spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", self.format)
                    .schema(self.data_schema)
                    .load(path))
        return df_cdc

    def save(self, df):
        stream = (df.writeStream
            .option("checkpointLocation", f"/Volumes/raw/{self.database}/cdc/{self.table}_checkpoint/")
            .option("header", "true")
            .option("cloudFiles.maxFilesPerTrigger", "500")
            .foreachBatch(lambda df, batchId: self.upsert(df))
            .trigger(availableNow=True)
        )
        return stream.start()
    


class ingestorCDF(ingestorCDC):

    def __init__(self, spark, catalog, database, table, id_field, idfield_old):
        
        super().__init__(spark=spark,
                        catalog=catalog,
                        database=database,
                        table=table,
                        data_format='delta',
                        id_field=id_field,
                        timestamp_field='_commit_timestamp')   
    
        self.idfield_old = idfield_old
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{database}/cdc/{catalog}_{table}_checkpoint/"

    def set_schema(self):
        return

    def set_query(self):
        query = utils.import_query(f"{self.table}.sql")
        self.from_table = utils.extract_from(query=query)
        self.original_query = query
        self.query = utils.format_query_cdf(query, "{df}")

    def load(self):
        df = (self.spark.readStream
                   .format('delta')
                   .option("readChangeFeed", "true")
                   .table(self.from_table))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                    .trigger(availableNow=True))
        return stream.start()
    
    def upsert(self, df):
        #df.createOrReplaceGlobalTempView(f"silver_{self.tablename}")

        #query_last = f"""
        #SELECT *
        #FROM global_temp.silver_{self.tablename}
        #WHERE _change_type <> 'update_preimage'
        #QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.idfield_old} ORDER BY _commit_timestamp DESC) = 1
        #"""
        #df_last = self.spark.sql(query_last)
        #df_upsert = self.spark.sql(self.query, df=df_last)

        df_filtered = df.filter(col("_change_type") != "update_preimage")
        window_spec = Window.partitionBy(self.idfield_old).orderBy(col(self._commit_timestamp).desc())

        query_last = df_filtered.withColumn("row_num", row_number().over(window_spec)) \
            .filter("row_num = 1") \
            .drop("row_num")
        
        
        df_last = self.spark.sql(query_last)
        df_upsert = self.spark.sql(self.query, df=df_last)


        (self.deltatable
            .alias("s")
            .merge(df_upsert.alias("d"), f"s.{self.id_field} = d.{self.id_field}") 
            .whenMatchedDelete(condition = "d._change_type = 'delete'")
            .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
            .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
            .execute())

    def execute(self):
        df = self.load()
        return self.save(df)


