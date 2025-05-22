# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class
    """

    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        transactionInputDF = get_data_source(
            data_type = "csv",
            file_path = "dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transactionInputDF.orderBy("customer_id", "transaction_date").show()

        customerInputDF = get_data_source(
            data_type= "delta",
            file_path="default.customer_delta_table_persist"
        ).get_data_frame()

        customerInputDF.show()

        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF
        }

        return inputDFs



