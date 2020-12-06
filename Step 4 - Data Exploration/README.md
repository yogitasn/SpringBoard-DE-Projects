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

![Alt text](FinalOccupancyFactTable.PNG?raw=true "Parking Occupancy")

Final Occupancy DateTime should look like This

![Alt text](OccupancyDateTimeDimensionTable.PNG?raw=true "Parking Occupancy")

BlockFace dataset
Displays blockfaces for all segments of the street network. Identifies the elements of the block, such as peak hour restrictions, length of the block, parking categories, and restricted parking zones.

![Alt text](BlockfaceDataset.PNG?raw=true "Parking Occupancy")

* Remove unwanted columns
* Rename columns name

Final BlockFace Schema should look like This

![Alt text](BlockFaceDimensionTable.PNG?raw=true "Parking Occupancy")

## Data Storage
### There are three processed files

<ol>
<li>Paid Parking Occupancy 2020-18GB</li>
<li>Blockface-8MB</li>
<li>Paid Parking Transaction-11MB</li>
</ol>

### There are three ways to store the processed data

<ol>
<li>CSV</li>
<li>Parquet</li>
<li>Avro</li>
</ol>

### The Paid Parking occupancy data generated for 2020 is less i.e. 18 GB (137M rows) is likely due to travel restrictions from the ongoing pandemic. The previous year 2019 data was on higher side i.e. 45GB(286M)
> CSV works well for a small data set of 70,000 lines. However, what if you need to increase to 2 million records? What if each record has nested properties? While CSV files are simple and human-readable, they unfortunately do not scale well. As the file size grows, load times become impractical, and reads cannot be optimized. Their primitive nature allows them to be a great option for smaller data sets, as shown above, but very inconvenient for managing larger sets of data. This is where both Parquet and Avro come in.


### Apache Parquet

>"Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk."

<ul>
<li>Since it’s a column based format, it’s better to use when you only need to access specific fields</li>
<li>Each data file contains the values for a set of rows</li>
<li>Can’t be written from streaming data since it needs to wait for blocks to get finished. However, this will work using micro-batch (eg Apache Spark).
</li>
<li>Suitable for data exploration — read intensive, complex or analytical querying, low latency data</li>
</ul>


### Apache Avro

> Apache Avro is a remote procedure call and data serialization framework developed within Apache’s Hadoop project. It uses JSON for defining data types and protocols, and serializes data in a compact binary format.

<ul>
<li>Since it’s a row based format, it’s better to use when all fields needs to be accessed</li>
<li>Files support block compression and are splittable</li>
<li>Can be written from streaming data (eg Apache Flink)</li>
<li>Suitable for write intensive operation</li>
</ul>

### Data Storage Format Conclusion

### For Seattle Parking Occupancy project Apache Parquet is better suited for data storage as we deal with immutable data and analytics queries, for which columnar storage is optimal.


## ERDiagram
![Alt text](SeattleParkingOccupancyERDiagram.PNG?raw=true "Data Model")
