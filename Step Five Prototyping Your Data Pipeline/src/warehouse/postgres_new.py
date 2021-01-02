#pyspark --driver-class-path /spark_drivers/postgresql-42.2.12.jar  --jars /spark_drivers/postgresql-42.2.12.jar

import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
findspark.find()
import os
jardrv = "~/spark_drivers/postgresql-42.2.12.jar"

spark = SparkSession.builder.config('spark.driver.extraClassPath', jardrv).getOrCreate()
url = 'jdbc:postgresql://127.0.0.1/testdb'
properties = {'user': 'postgres', 'password': 'Quark@2416'}
df = spark.read.jdbc(url=url, table='testtable', properties=properties)

import os
from pyspark.sql import DataFrameWriter

df1=spark.createDataFrame([('480','788'),('679','888')],['ts','ts1'])

#set UTC timestamp
spark.sql("set spark.sql.session.timeZone=UTC")

my_writer = DataFrameWriter(df1)

database_name = 'testdb'
table = "testtable1"
hostname = 'localhost'
mode = "overwrite"

my_writer.jdbc(url, table, mode, properties)

