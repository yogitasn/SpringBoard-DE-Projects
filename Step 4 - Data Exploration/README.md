## Table of contents
* [General Info](#general-info)
* [Exploratory Data Analysis](#Exploratory-Data-Analysis)
* [Data Storage](#datastorage)
* [ER Diagram](#ERDiagram)


## General Info
This project is Data Exploration of the open ended Data Engineering Project: Seattle Parking Occupancy

## Description
Original dataset is 18GB in size and hence a smaller subset of the data was downloaded and analyzed for this Project

Street Parking Occupancy data (Processed data-Last 48 hours)
[Data Source Link](https://data.seattle.gov/Transportation/Paid-Parking-Last-48-Hours-/hiyf-7edq)

![Alt text](ParkingOccupancyFirstFewRecs.PNG?raw=true "Parking Occupancy")

Checked for datatypes of columns and null values
* 'PaidParkingSubArea' have blank values as every Parking Area doesn't have a sub-area
* 'PaidParkingRate' have null values and can be dropped as this information will be captured via Blockface dataset
* There are some Blank and out of range values like 4320 in the 'ParkingTimeLimitCategory' column. These values match with the Blockface column data.
* Column name rename 'SourceElementKey'-> 'Station_ID','PaidOccupancy'>'Occupied_Spots','ParkingSpaceCount'>'Available_Spots'
* Remove the comma from the Station_ID
* Create the OccupancyTimeData Dimension Table using OccupancyDateTime and derive the additional columns such as 'month','day of the week','hour' using Pyspark to get the monthly,daily,hourly trends
* Drop unwanted columns

Final Occupancy Fact Table should look like This

![Alt text](ParkingOccupancyFirstFewRecs.PNG?raw=true "Parking Occupancy")

Final Occupancy DateTime should look like This
![Alt text](ParkingOccupancyFirstFewRecs.PNG?raw=true "Parking Occupancy")




## DataModel
![Alt text](/screenshot/datamodel/DataModel.PNG?raw=true "Data Model")

## Technologies
Project is created with:
* MySQL Database
* Flask-SQLALchemy
* Flask (Only Backend)
* PyMySQL (Python library connector to communicate with MYSQL )


## Setup

Run the following SQL command  

```
create database <DB_NAME>

```
To update the configuration file 'database.cfg' with your database credentials and DB name created in above step.

```
[DATABASE]
DB_USER=
DB_PASSWORD=
DB_PORT=
DATABASE=<DB_NAME>

```

## Execution

Navigate to project folder and execute the following commands

Using Python 3.7+, run `pip3 install -r requirements.txt` to install the dependencies.

Execute Commands:

* `flask initdb`
* `flask bootstrap`


## References
[Flask SQLALchemy](https://flask-sqlalchemy.palletsprojects.com/en/2.x/)
