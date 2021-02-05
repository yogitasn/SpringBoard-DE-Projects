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

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
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


raw = spark.sparkContext.textFile("C:\\SpringBoard-DE-Projects\\Step 2 - Data Ingestion\\data\\json\\2020-08-05\\NASDAQ\\*.txt")
parsed = raw.map(lambda line: parse_json(line))
data = spark.createDataFrame(parsed,schema=common_event)
data.show(10)


import json
def parse_json(line:str):
    record = json.loads(line)
    record_type = record['event_type']
    trade_dt= datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d')
    rec_type=record_type
    symbol=record['symbol']
    exchange=record['exchange']
    event_tm=datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
    event_seq_nb=int(record['event_seq_nb'])         
    try:    
        if record_type == "T":
            trade_pr=decimal.Decimal(record['price'])
            #if # [some key fields empty]
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0,trade_pr,decimal.Decimal(0.0),0,decimal.Decimal(0.0),0,"T")
            return event
        #else :
             #   event = ("","","","","","","","","B",line)
            #return event
        elif record_type == "Q":
            bid_pr=decimal.Decimal(record['bid_pr'])
            bid_size=int(record['bid_size'])
            ask_pr=decimal.Decimal(record['ask_pr'])
            ask_size=int(record['ask_size'])
        # [Get the applicable field values from json]
            #if # [some key fields empty]
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,0,decimal.Decimal(0.0),bid_pr,bid_size,ask_pr,ask_size,"Q")
            #else :
            #event = ("","","","","","","","","B",line)
            return event
    except Exception as e:
            # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string
            return ("","","","","","","","","B",line)
