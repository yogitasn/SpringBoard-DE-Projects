from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import parkingOccpn_udf
import logging
import configparser
from pathlib import Path
import pandas as pd
import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('..\config.cfg')

class ParkingOccupancyTransform:
    """
    This class performs transformation operations on the dataset.
    1. Transform timestamp format, clean text part, remove extra spaces etc.
    2. Create a lookup dataframe which contains the id and the timestamp for the latest record.
    3. Join this lookup data frame with original dataframe to get only the latest records from the dataset.
    4. Save the dataset by repartitioning. Using gzip compression
    """

    def __init__(self):
        self._load_path = config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = config.get('BUCKET', 'PROCESSED_ZONE')

    def transform_load_parking_occupancy(self):
        logging.debug("Inside transform parking occupancy dataset module")
        
        schema = StructType() \
                .add("OccupancyDateTime",StringType(),True) \
                .add("Occupied_Spots",IntegerType(),True) \
                .add("BlockfaceName",StringType(),True) \
                .add("SideOfStreet",StringType(),True) \
                .add("Station_Id",StringType(),True) \
                .add("ParkingTimeLimitCategory",IntegerType(),True) \
                .add("Available_Spots",IntegerType(),True) \
                .add("PaidParkingArea",StringType(),True) \
                .add("PaidParkingSubArea",StringType(),True) \
                .add("PaidParkingRate",DoubleType(),True) \
                .add("ParkingCategory",StringType(),True) \
                .add("Location",StringType(),True)

       
                
        occ_df = spark.read.format("csv") \
                    .option("header", True) \
                    .schema(schema) \
                    .load(self._load_path+"2020_Paid_Parking.csv")

        
        occ_df=occ_df.withColumn('Station_Id',parkingOccpn_udf.commaRep('Station_Id'))

        occ_df = occ_df.withColumn("Station_Id",\
                        occ_df["Station_Id"].cast(IntegerType()))

        
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        occ_df = occ_df.withColumn("OccupancyDateTime", \
                       F.to_timestamp(occ_df.OccupancyDateTime, format="mm/dd/yyyy hh:mm:ss a"))
        
        occ_df = occ_df.drop("BlockfaceName",
                   "SideOfStreet",
                   "ParkingTimeLimitCategory",
                   "PaidParkingArea",
                   "PaidParkingSubArea",
                   "PaidParkingRate",
                   "ParkingCategory")
 

        occ_df=occ_df.withColumn('Longitude',F.split('Location',' ').getItem(1)) \
                     .withColumn('Latitude',F.split('Location',' ').getItem(2))
        
        occ_df=occ_df.withColumn('Latitude',parkingOccpn_udf.braceRepl('Latitude')) \
                     .withColumn('Longitude',parkingOccpn_udf.braceRepl('Longitude'))

        
        occ_df = occ_df.withColumn("Latitude",occ_df["Latitude"].cast(DoubleType())) \
                       .withColumn("Longitude",occ_df["Longitude"].cast(DoubleType()))
        
        #occ_df.write.mode("overwrite").parquet("C:\\Files\\parking_occupancy.parquet")

       pg=PostgresConnector()
       pg.write(occ_df,'occupancy', "overwrite"):
        

    def transform_load_blockface_dataset(self):
        logging.debug("Inside transform blockface dataset module")
        schema = StructType() \
                .add("objectid",IntegerType(),True) \
                .add("station_id",IntegerType(),True) \
                .add("segkey",IntegerType(),True) \
                .add("unitid",IntegerType(),True) \
                .add("unitid2",IntegerType(),True) \
                .add("station_address",StringType(),True) \
                .add("side",StringType(),True) \
                .add("block_id",StringType(),True) \
                .add("block_nbr",IntegerType(),True) \
                .add("csm",StringType(),True) \
                .add("parking_category",StringType(),True) \
                .add("load",IntegerType(),True) \
                .add("zone",IntegerType(),True) \
                .add("total_zones",IntegerType(),True) \
                .add("wkd_rate1",DoubleType(),True) \
                .add("wkd_start1",IntegerType(),True) \
                .add("wkd_end1",IntegerType(),True) \
                .add("wkd_rate2",DoubleType(),True) \
                .add("wkd_start2",StringType(),True) \
                .add("wkd_end2",StringType(),True) \
                .add("wkd_rate3",DoubleType(),True) \
                .add("wkd_start3",StringType(),True) \
                .add("wkd_end3",StringType(),True) \
                .add("sat_rate1",DoubleType(),True) \
                .add("sat_start1",StringType(),True) \
                .add("sat_end1",StringType(),True) \
                .add("sat_rate2",DoubleType(),True) \
                .add("sat_start2",StringType(),True) \
                .add("sat_end2",StringType(),True) \
                .add("sat_rate3",DoubleType(),True) \
                .add("sat_start3",StringType(),True) \
                .add("sat_end3",StringType(),True) \
                .add("rpz_zone",StringType(),True) \
                .add("rpz_area",DoubleType(),True) \
                .add("paidarea",StringType(),True) \
                .add("parking_time_limit",DoubleType(),True) \
                .add("subarea",StringType(),True) \
                .add("start_time_wkd",StringType(),True) \
                .add("end_time_wkd",StringType(),True) \
                .add("start_time_sat",StringType(),True) \
                .add("end_time_sat",StringType(),True) \
                .add("primarydistrictcd",StringType(),True) \
                .add("secondarydistrictcd",StringType(),True) \
                .add("overrideyn",StringType(),True) \
                .add("overridecomment",IntegerType(),True) \
                .add("shape_length",DoubleType(),True) 

        
        blockface = spark.read.format("csv") \
                        .option("header", True) \
                        .schema(schema) \
                        .load("C:\\FIles\\BlockFace.csv")
    

        columns_to_drop = ["objectid","segkey",
                        "unitid", "unitid2",
                        "block_id","csm",
                        "load","zone",
                        "total_zones","rpz_zone",
                        "rpz_area","paidarea",
                        "start_time_wkd","end_time_wkd",
                        "start_time_sat","end_time_sat",
                        "primarydistrictcd","secondarydistrictcd",
                        "overrideyn","overridecomment",
                        "shape_length"]

        blockface=blockface.drop(*columns_to_drop)


        blockface_df = blockface_df.drop(,axis=1)
       
    

        blockface=blockface.withColumn('wkd_start1',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_start1')) \
                        .withColumn('wkd_end1',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_end1')) \
                        .withColumn('wkd_start2',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_start2')) \
                        .withColumn('wkd_end2',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_end2')) \
                        .withColumn('wkd_start3',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_start3')) \
                        .withColumn('wkd_end3',parkingOccpn_udf.udf_format_minstoHHMMSS('wkd_end3')) \
                        .withColumn('sat_start1',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_start1')) \
                        .withColumn('sat_end1',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_end1')) \
                        .withColumn('sat_start2',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_start2')) \
                        .withColumn('sat_end2',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_end2')) \
                        .withColumn('sat_start3',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_start3')) \
                        .withColumn('sat_end3',parkingOccpn_udf.udf_format_minstoHHMMSS('sat_end3'))
                               
                  
       pg=PostgresConnector()
       pg.write(blockface,'blockface', "overwrite"):
       
                    