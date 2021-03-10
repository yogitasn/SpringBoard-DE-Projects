## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)
* [Execution](#execution)

## General Info
This project is Apache Airflow mini project-2

## Description
In this mini project, we will exercise the text processing techniques using Python. Parsing text files and getting useful information from logs is a common practice in real life projects. 

## Technologies
Project is created as follows:
* Python3.7+ 


## Setup

Airflow logs is saved locally to the following path

![Alt text](Screenshots/airflowlogpath.PNG?raw=true "Airflow Log Path")

Navigate to the project folder and execute the log analyzer python file using the below command

```
python3 log_analyzer.py <path_of_airflow_logs>

```
It will iterate through all the log files and generate the below output

![Alt text](Screenshots/log_analyzer_output.PNG?raw=true "Log Analyzer output")

Refer the executionoutput.txt for the full execution log