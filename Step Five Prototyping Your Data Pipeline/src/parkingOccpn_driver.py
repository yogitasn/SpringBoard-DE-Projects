from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from .warehouse import ParkingWarehouseDriver

def main():
    """
    This method performs below tasks:
    1: Check for data in Landing Zone, if new files are present move them to Working Zone
    2: Transform data present in working zone and save the transformed data to Processed Zone
    3: Run Data Warehouse functionality by setting up Staging tables, then loading staging tables, performing upsert operations on warehouse.
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    pot = ParkingOccupancyTransform()

    # Modules in the project
    modules = {
        "parkingOccupancy.csv": pot.transform_load_parking_occupancy,
        "blockface.csv" : pot.transform_load_blockface_dataset
    }

  
    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # Starting warehouse functionality
    prwarehouse = ParkingWarehouseDriver()
    logging.debug("Setting up staging tables")
    prwarehouse.setup_staging_tables()
    logging.debug("Populating staging tables")
    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()

# Entry point for the pipeline
if __name__ == "__main__":
    main()