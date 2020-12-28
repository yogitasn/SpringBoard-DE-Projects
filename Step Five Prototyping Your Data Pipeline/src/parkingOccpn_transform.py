from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import goodreads_udf
import logging
import configparser
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

class ParkingOccupancyTransform:
    """
    This class performs transformation operations on the dataset.
    1. Transform timestamp format, clean text part, remove extra spaces etc.
    2. Create a lookup dataframe which contains the id and the timestamp for the latest record.
    3. Join this lookup data frame with original dataframe to get only the latest records from the dataset.
    4. Save the dataset by repartitioning. Using gzip compression
    """

    def __init__(self):
        self._load_path = 's3a://' + config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = 's3a://' + config.get('BUCKET', 'PROCESSED_ZONE')

    def transform_parking_occupancy(self):
        logging.debug("Inside transform parking occupancy dataset module")
        parking_occ_df = pd.\
                         read_csv(self._load_path + '/author.csv', header=True, mode='PERMISSIVE',inferSchema=True)
 
        #parking_occ_df[['POINT','Longitude','Latitude']]=parking_occ_df.\
         #                                                Location.str.split(" ",expand=True)

        parking_occ_df['Longitude']=parking_occ_df.Location.map(lambda x: x.split(' ')[1])

        parking_occ_df['Latitude']=parking_occ_df.Location.map(lambda x: x.split(' ')[2])
        
        parking_occ_df['Latitude']=parking_occ_df.\
                                   Latitude.str.replace(')','')

        parking_occ_df['Longitude']=parking_occ_df.\
                                    Longitude.str.replace('(','')
                    
    
        parking_occ_df=parking_occ_df.drop(['POINT','Location','BlockfaceName',\
                                           'SideOfStreet','ParkingTimeLimitCategory',\
                                           'PaidParkingArea','PaidParkingSubArea',\
                                           'PaidParkingRate','ParkingCategory'],axis=1)

        parking_occ_df.rename(columns = {'SourceElementKey':'Station_ID',\
                                        'PaidOccupancy':'Occupied_Spots',\
                                        'ParkingSpaceCount':'Available_Spots'}, inplace = True) 

        
        occupancy_df=parking_occ_df['OccupancyDateTime']

    
        occupancy_df['month'] = pd.DatetimeIndex(df['OccupancyDateTime']).month 
  
        # get month from the corresponding  
        # birth_date column value 
        occupancy_df['day_of_week'] = pd.DatetimeIndex(df['OccupancyDateTime']).day 
        
        occupancy_df['hour'] = pd.DatetimeIndex(df['OccupancyDateTime']).hour

        parking_occ_df.to_csv('parking_occ_df_proc.csv')

        occupancy_df.to_csv('occupancy_df.csv')




    def transform_blockface_dataset(self):
        logging.debug("Inside transform blockface dataset module")
        blockface_df =pd.\
                     read_csv( self._load_path + '/author.csv', header=True, mode='PERMISSIVE',inferSchema=True)

        blockface_df = blockface_df.drop(["OBJECTID","SEGKEY",
                                         "UNITID", "UNITID2",
                                        "BLOCK_ID","CSM",
                                        "LOAD","ZONE",
                                        "TOTAL_ZONES","RPZ_ZONE",
                                        "RPZ_AREA","PAIDAREA",
                                        "START_TIME_WKD","END_TIME_WKD",
                                        "START_TIME_SAT","END_TIME_SAT",
                                        "PRIMARYDISTRICTCD","SECONDARYDISTRICTCD",
                                        "OVERRIDEYN","OVERRIDECOMMENT",
                                        "SHAPE_Length"],axis=1)
        
        blockface_df.rename(columns={'ELMNTKEY':'Station_ID','UNITDESC':'UnitDesc','SIDE':'Side',
                                 'BLOCK_NBR':'Block_Nbr','PARKING_CATEGORY':'Parking_Category',
                                 'WKD_RATE1':'Wkd_Rate1','WKD_START1':'Wkd_Start1',
                                 'WKD_END1':'Wkd_End1','WKD_RATE2':'Wkd_Rate2',
                                 'WKD_START2':'Wkd_Start2','WKD_END2':'Wkd_End2',
                                 'WKD_RATE3':'Wkd_Rate3','WKD_START3':'Wkd_Start3',
                                 'WKD_END3':'Wkd_End3','SAT_RATE1':'Sat_Rate1',
                                 'SAT_START1':'Sat_Start1','SAT_END1':'Sat_End1',
                                 'SAT_RATE2':'Sat_Rate2','SAT_START2':'Sat_Start2',
                                 'SAT_END2':'Sat_End2','SAT_RATE3':'Sat_Rate3',
                                 'SAT_START3':'Sat_Start3','SAT_END3':'Sat_End3',
                                 'PARKING_TIME_LIMIT':'Parking_Time_Limit',
                                 'SUBAREA':'SubArea'},inplace=True)

        blockface_df.to_parquet('blockface_df.parquet.gzip', compression='gzip')
                    