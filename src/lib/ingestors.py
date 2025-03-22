
import delta
import utils




class ingestor:

    def __init__(self,spark,catalog,database,table,data_format,id_field,timestamp_field):
        
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.format = data_format
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(table)

    def load(self,path):
        self.df = self.spark.read.format(self.format).schema(self.data_schema).option("header", "true").load(path)
        return self.df

    def save(self,df):
        (df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.database}.{self.table}"))
        return True
        
    def execute(self,path):
        df = self.load(path)
        return self.save(df)
    



class ingestorCDC(ingestor):

            def __init__(self,spark,catalog,database,table,data_format,id_field,timestamp_field):

                super().__init__(spark,catalog,database,table,data_format,id_field,timestamp_field)

                self.id_field = id_field
                self.timestamp_field = timestamp_field

                self.set_deltatable()

            def set_deltatable(self):
                table = f"{self.catalog}.{self.database}.{self.table}"
                self.deltatable = delta.DeltaTable.forName(self.spark,table)   

            def upsert(self, df):
            
            window_spec = Window.partitionBy(f"{self.id_field}").orderBy(col(f"{self.timestamp_field}").desc())
            df_cdc_unique = self.spark.df_cdc.withColumn("row_num", row_number().over(window_spec)) \
                        .filter("row_num = 1") \
                        .drop("row_num")

            (self.deltatable
                .alias("b")
                .merge(df_cdc_unique.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
                .whenMatchedDelete(condition= "d.OP = 'D'")
                .whenMatchedUpdateAll(condition= "d.OP = 'U'")
                .whenNotMatchedInsertAll(condition= "d.OP = 'I' OR d.OP = 'U'")
                .execute()
            )

            def load(self, path):
                df_cdc = (self.spark.readStream\
                                .format("cloudFiles")\
                                .option("cloudFiles.format", self.format)\
                                .schema(self.data_schema)\
                                .load(path)
        )
            def save(self,df):
                stream = (df.writeStream\
                    .option("checkpointLocation",f"/Volumes/raw/{self.database}/cdc/{self.table}_checkpoint/") \
                    .option("header", "true")\
                    .option("cloudFiles.maxFilesPerTrigger", "500")
                    .foreachBatch(lambda df, batchId: self.upsert(df))
                    .trigger(availableNow=True))
                return stream.start()
            



      


    

