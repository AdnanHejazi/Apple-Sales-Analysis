# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF,
            path = "dbfs:/FileStore/tables/output/airpodsAfterIphone",
            mode = "overwrite"
        ).load_data_frame()
        

class OnlyAirPodsAndIphoneLoader(AbstractLoader):
    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.transformedDF,
            path = "dbfs:/FileStore/tables/output/onlyAirpodsAndIphone",
            mode = "overwrite",
            params = params
        ).load_data_frame()