## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)


## General Info
This project is Data Ingestion step of Guided Capstone project

## Description
After preprocessing the incoming data from the exchange, we need to create the final data format to store on the cloud. The cloud will also store historic exchange data, so Spring Capital can look up any trading day and easily find historic data.
T
This preprocessed data will be used in the following ETL process, as well as for adhoc user queries.


## Technologies
Project is created in Azure with the following:
* Azure Storage Container
* Azure HDInsight


## Setup

'End-of-Day_Data_Load_Notebook' has the Pyspark code to work on the raw preprocessed parquet files, apply correction and save it back to Azure storage

Also, the detailed steps and screenshots are in the document 'End-of-Day Data Load Screenshot'

