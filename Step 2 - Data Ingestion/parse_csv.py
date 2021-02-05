import findspark

findspark.init()

findspark.find()


findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

    
from typing import List
import datetime
import decimal



common_event = StructType() \
              .add("trade_dt",DateType(),True) \
              .add("rec_type",StringType(),True) \
              .add("symbol",StringType(),True) \
              .add("exchange",StringType(),True) \
              .add("event_tm",TimestampType(),True) \
              .add("event_seq_nb",IntegerType(),True) \
              .add("arrival_tm",IntegerType(),True) \
              .add("trade_pr",DecimalType(),True) \
              .add("bid_pr",DecimalType(),True) \
              .add("bid_size",IntegerType(),True) \
              .add("ask_pr",DecimalType(),True) \
              .add("ask_size",IntegerType(),True) \
              .add("partition",StringType(),True)

spark = SparkSession.builder.master('local').appName('app').getOrCreate()



raw = spark.sparkContext.textFile("C:\\SpringBoard-DE-Projects\\Step 2 - Data Ingestion\\data\\csv\\2020-08-05\\NYSE\\*.txt")

raw.take(3)
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed,schema=common_event)

data.show(20)
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

decimal.getcontext().prec = 10

def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    trade_dt= datetime.datetime.strptime(record[0], '%Y-%m-%d')
    rec_type=record[2]
    symbol=record[3]
    exchange=record[6]
    event_tm=datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
    event_seq_nb=int(record[5])
    try:
        if record[record_type_pos] == "T":
            trade_pr=decimal.Decimal(record[7])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0,trade_pr,\
                    decimal.Decimal(0.0),0,decimal.Decimal(0.0),0,"T")
            return event
        elif record[record_type_pos] == "Q":
            bid_pr=decimal.Decimal(record[7])
            bid_size=int(record[8])
            ask_pr=decimal.Decimal(record[9])
            ask_size=int(record[10])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0, \
                    decimal.Decimal(0.0),bid_pr,bid_size,ask_pr,ask_size,"Q")
            return event
    except Exception as e:
            return ("","","","","","","","","B",line)