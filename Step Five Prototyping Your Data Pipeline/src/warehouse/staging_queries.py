import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read('.\warehouse_config.cfg')


# Setup Drop table queries
drop_occupancy_table = """DROP TABLE IF EXISTS occupancy;"""
drop_blockface_table = """DROP TABLE IF EXISTS blockface;"""

create_occupancy_table = """
CREATE TABLE IF NOT EXISTS occupancy
(
    OccupancyDateTime TIMESTAMP,
    Available_Spots INT,
    Station_Id INT,
    Occupied_Spots INT,
    Latitude DECIMAL(3,2),
    Longitude DECIMAL(3,2),
    PRIMARY KEY(OccupancyDateTime,Latitude,Longitude)
)
;
"""

create_blockface_table = """
CREATE TABLE IF NOT EXISTS blockface
(
    station_id INT,
    station_address VARCHAR(30),
    side VARCHAR(10),
    block_nbr INT,
    parking_category VARCHAR(10),
    wkd_rate1 DECIMAL(3,2),
    wkd_start1 VARCHAR(10),
    wkd_end1 VARCHAR(10), 
    wkd_rate2 DECIMAL(3,2),
    wkd_start2 VARCHAR(10),
    wkd_end2 VARCHAR(10),
    wkd_rate3 DECIMAL(3,2),
    wkd_start3 VARCHAR(10),
    wkd_end3 VARCHAR(10),
    sat_rate1 DECIMAL(3,2),
    sat_start1 VARCHAR(10),
    sat_end1 VARCHAR(10),
    sat_rate2 DECIMAL(3,2),
    sat_start2 VARCHAR(10),
    sat_end2 VARCHAR(10),
    sat_rate3 DECIMAL(3,2),
    sat_start3 VARCHAR(10),
    sat_end3 VARCHAR(10),
    parking_time_limit INT,
    subarea VARCHAR(20)

)
;
"""

drop_staging_tables = [drop_occupancy_table, drop_blockface_table]
create_staging_tables = [create_occupancy_table, create_blockface_table]
