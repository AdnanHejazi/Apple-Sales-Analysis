# Databricks notebook source
# MAGIC %run "./transformer"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader  "

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL pipeline to generate output data that shows all customers who bought Airpods after purchasing an iPhone
    """
    def __init__(self):
        pass

    def runner(self):
        #Extract:
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Transform:
        #Customers who have bought Airpods after buying the iPhone
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)

        #Load:
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()

# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate output data that shows all customers who bought Airpods after purchasing an iPhone
    """
    def __init__(self):
        pass

    def runner(self):

        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Customers who have bought Airpods after buying the iPhone
        secondTransformedDF = IphoneAndAirpodsTransformer().transform(inputDFs)

        OnlyAirPodsAndIphoneLoader(secondTransformedDF).sink()

# COMMAND ----------

class WorkFlowRunner:

    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFlow().runner()
        else
        
name = "secondWorkFlow"

workFlowRunner = WorkFlowRunner(name).runner()
        