from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database_name = 'occupancy'
        self.hostname = 'localhost'
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {"user":"", 
                      "password":'Quark@2416',
                      "driver": "org.postgresql.Driver"
                     }
    def get_writer(self, df):
        return DataFrameWriter(df)
        
    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)