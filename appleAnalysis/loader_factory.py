# Databricks notebook source
class DataSink:
    """
    Abstract class
    """

    def __init__(self, df, path, mode, params):
        self.df = df
        self.path = path
        self.mode = mode
        self.params = params

    def load_data_frame(self):
        """
        Abstract method, Functions will be defined in sub classes
        """

        raise ValueError("Not Implemented")

class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.mode).save(self.path)

class LoadToDBFSWithPartition(DataSink):

    def load_data_frame(self):
        
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.mode).partitionBy(*partitionByColumns).save(self.path)

def get_sink_source(sink_type, df, path, mode, params = None):
    if sink_type == "dbfs":
        return LoadToDBFS(df, path, mode, params)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartition(df, path, mode, params)
    else:
        return ValueError(f"Not Implemented for sink type:")
        