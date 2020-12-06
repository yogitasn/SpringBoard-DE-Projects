## Table of contents
* [General Info](#general-info)
* [Exploratory Data Analysis](#Exploratory-Data-Analysis)
* [Data Storage](#datastorage)
* [ER Diagram](#ERDiagram)


## General Info
This project is Data Exploration of the open-ended Data Engineering Project: Seattle Parking Occupancy

## Exploratory Data Analysis
The original dataset is 18GB in size and hence a smaller subset(last 48 hours) of the data was downloaded and analyzed for this Project

Street Parking Occupancy data
[Data Source Link](https://data.seattle.gov/Transportation/Paid-Parking-Last-48-Hours-/hiyf-7edq)


* First Few Records
![Alt text](./images/ParkingOccupancyFirstFewRecs.PNG?raw=true "Parking Occupancy")


Below are some of the observations on EDA of the Dataset

* 'PaidParkingSubArea' column has blank values as every Parking Area doesn't have a sub-area
* 'PaidParkingRate' column has null values and can be dropped as this information will be captured via Blockface dataset
* There are some blank and out of range values like '4320' in the 'ParkingTimeLimitCategory' column. These values match with the master Blockface dataset column and hence can be ignored.
* Column name to be renamed as follows: 'SourceElementKey'-> 'Station_ID','PaidOccupancy'>'Occupied_Spots','ParkingSpaceCount'>'Available_Spots'
* To remove the comma from the rows in the column 'Station_ID'
* Create the OccupancyTimeData Dimension Table using OccupancyDateTime and derive the additional columns such as 'month', 'day of the week', 'hour' using Pyspark to get the monthly, daily, hourly trends
* Drop redundant columns

Below is final Occupancy Fact Table Schema:

![Alt text](./images/FinalOccupancyFactTable.PNG?raw=true "Parking Occupancy")

Below is final Occupancy DateTime Dimension Table Schema:

![Alt text](./images/OccupancyDateTimeDimensionTable.PNG?raw=true "DateTime")

### BlockFace Dataset
Displays block faces for all segments of the street network. Identifies the elements of the block, such as peak hour restrictions, length of the block, parking categories, and restricted parking zones.

[Data Source Link](https://data-seattlecitygis.opendata.arcgis.com/datasets/a1458ad1abca41869b81f7c0db0cd777_0)

* First Few Records
![Alt text](./images/BlockfaceDataset.PNG?raw=true "BlockFace")


Below are some of the observations on EDA of the dataset

* Remove redundant columns
* Rename columns name
* There are blank values for certain columns like WKD_RATE3/WKDSTART3/WKDEND3/SAT_RATE1 as there could be no parking permitted during that time period in those areas
* Station_ID in Parking Occupancy Table matches with Station_ID in Blockface.
* Blockface has additional details about the block such as peak hour restrictions, length of the block, parking categories, and restricted parking zones.

Final BlockFace Schema:

![Alt text](./images/BlockFaceDimensionTable.PNG?raw=true "BlockFace")

## Data Storage
### Below are the processed files

<ol>
<li>Paid Parking Occupancy 2020-18GB</li>
<li>Blockface-8MB</li>
</ol>

### There are three ways to store the processed data

<ol>
<li>CSV</li>
<li>Parquet</li>
<li>Avro</li>
</ol>

### The Paid Parking occupancy data generated for 2020 is lesser than the previous year i.e. 18 GB (137M rows) is likely due to travel restrictions from the ongoing pandemic. The previous year 2019 data was on the higher side i.e. 45GB(286M)
> CSV works well for a small data set of 70,000 lines. However, what if you need to increase to 2 million records? What if each record has nested properties? While CSV files are simple and human-readable, they, unfortunately, do not scale well. As the file size grows, load times become impractical, and reads cannot be optimized. Their primitive nature allows them to be a great option for smaller data sets, but very inconvenient for managing larger sets of data. This is where both Parquet and Avro come in.


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

> Apache Avro is a remote procedure call and data serialization framework developed within Apache’s Hadoop project. It uses JSON for defining data types and protocols and serializes data in a compact binary format.

<ul>
<li>Since it’s a row based format, it’s better to use when all fields needs to be accessed</li>
<li>Files support block compression and are splittable</li>
<li>Can be written from streaming data (eg Apache Flink)</li>
<li>Suitable for write intensive operation</li>
</ul>

### Data Storage Format Conclusion

### For the Seattle Parking Occupancy project 'Apache Parquet' is better suited for data storage as we deal with immutable data and analytical queries, for which columnar storage is optimal.


## ER Diagram
Below is final ER Diagram

![Alt text](./images/SeattleParkingOccupancyERDiagram.PNG?raw=true "ERDiagram")
