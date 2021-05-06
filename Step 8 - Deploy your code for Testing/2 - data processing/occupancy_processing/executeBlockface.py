import findspark

findspark.init()

findspark.find()

import pyspark

findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
import logging
import configparser
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
import re

import json

from occupancy_processing import miscProcess

from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
import re
import time

SCRIPT_NAME= os.path.basename(__file__)


def global_SQLContext(spark1):
    global spark
    spark= spark1

def global_EffectDt(iEffevtDt):
    global EffectvDt

    EffectvDt=datetime.strptime(iEffevtDt, "%Y-%m%d").date()

def get_currentDate():
    current_time = datetime.now()
    str_current_time = current_time.strftime("%Y-%m-%d")
    miscProcess.log_print(str_current_time)

def regex_replace_values():
    regexp_replace

    
@udf(StringType())
def format_minstoHHMMSS(x):
    """
    Function to convert the minutes to HH:MM:SS format
    """
    try:
        duration=timedelta(minutes=int(x))
        seconds = duration.total_seconds()
        minutes = seconds // 60
        hours = minutes // 60
        return "%02d:%02d:%02d" % (hours, minutes % 60, seconds % 60)
    except:
        None

""" Function to read blockface csv file using file path and schema """
def sourceBlockfaceReadParquet(blockfacefilePath, cust_schema):
  
    
    miscProcess.log_info(SCRIPT_NAME, "Reading CSV file...")
    print("Reading CSV file")

    source_data_info = {}
    source_data_info["type"] = "CSV"


    try:
        blockface = spark.read.format("csv") \
                    .option("header", True) \
                    .schema(cust_schema) \
                    .load(blockfacefilePath)


    except Exception as e:
        miscProcess.log_info(SCRIPT_NAME, "error in reading csv: {}".format(e))
    
   
    source_data_info["blockfacefilePath"] = blockfacefilePath

    return (blockface, source_data_info)


""" Function to perform transformations on the dataset """
def executeBlockfaceOperations(src_df, output, cols_list, max_retry_count,retry_delay):

    miscProcess.log_print("Starting the Blockface Execute Operations")

    src_df.printSchema()

    ReturnCode=0
    rec_cnt=0
    RetryCt =0
    Success=False
    
    while(RetryCt < max_retry_count) and not Success:
        
        try:
            Success = True
            input_df = src_df
        
        except:
            Success = False
            RetryCt +=1
            if RetryCt == max_retry_count:
                miscProcess.log_info(SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count))
                ReturnCode=1
                return ReturnCode, rec_cnt

            else:
                miscProcess.log_info(SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay))

    
    select_df = input_df.select([colname for colname in input_df.columns if colname in (cols_list)])

    

    select_df=select_df.withColumn('wkd_start1',format_minstoHHMMSS('wkd_start1')) \
                    .withColumn('wkd_end1',format_minstoHHMMSS('wkd_end1')) \
                    .withColumn('wkd_start2',format_minstoHHMMSS('wkd_start2')) \
                    .withColumn('wkd_end2',format_minstoHHMMSS('wkd_end2')) \
                    .withColumn('wkd_end3',format_minstoHHMMSS('wkd_end3')) \
                    .withColumn('sat_start1',format_minstoHHMMSS('sat_start1')) \
                    .withColumn('sat_end1',format_minstoHHMMSS('sat_end1')) \
                    .withColumn('sat_start2',format_minstoHHMMSS('sat_start2')) \
                    .withColumn('sat_end2',format_minstoHHMMSS('sat_end2')) \
                    .withColumn('sat_start3',format_minstoHHMMSS('sat_start3')) \
                    .withColumn('sat_end3',format_minstoHHMMSS('sat_end3'))
        

    #miscProcess.log_print("Writing to output file: {}".format(output))

    select_df = select_df.select([colname for colname in input_df.columns if colname in (cols_list)])

    RetryCt = 0
    Success = False

    while(RetryCt < max_retry_count) and not Success:
        try:
            Success = True
            miscProcess.log_info(SCRIPT_NAME, "Writing to Parquet file")
            select_df.show(3)
            print("Output file {}".format(output))
            select_df.coalesce(1).write.mode("overwrite").parquet(output+"//Blockface.parquet")
        except:
            Success = False
            RetryCt += 1
            if RetryCt == max_retry_count:
                miscProcess.log_info(SCRIPT_NAME, "Failed on writing File after {} tries: {} ".format(max_retry_count, output))
                ReturnCode = 2
                return ReturnCode, rec_cnt
            else:
                miscProcess.log_info(SCRIPT_NAME, "Failed on writing File, re-try in {} seconds ".format(retry_delay))
                time.sleep(retry_delay)

    miscProcess.log_print("Number of Records Processed: {}".format(rec_cnt))
    
    return ReturnCode, rec_cnt

def main():

    executeBlockfaceOperations(src_df, output, cols_list, max_retry_count,retry_delay)


if __name__=='__main__':
    
    logfile = 'test_123.log'
    miscProcess.initial_log_file(logfile)
    #miscProcess.complete_log_file(logfile)
    main()


            






    



