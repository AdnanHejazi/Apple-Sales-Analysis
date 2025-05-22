# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast


class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """

        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")

        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Customer buying Airpods after purchasing iPhone")
        transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

        filteredDF = transformedDF.filter(
            (col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods')
        )

        filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()


        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )
        print("JOINED DF")
        joinDF.show()

        return joinDF
    
class IphoneAndAirpodsTransformer(Transformer):
    def transform(self, inputDFs):

        transactionInputDF = inputDFs.get("transactionInputDF")
        print("transactionInputDF in transform")

        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )
        
        groupedDF.show()

        filteredDF = transformedDF.filter(
            (col("products").contains("iPhone")) &
            (col("products").contains("Airpods")) &
            (col("products").size() == 2)
        )

        filteredDF.show()


        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
        )
        print("JOINED DF")
        joinDF.show()

        return joinDF
        


# COMMAND ----------

