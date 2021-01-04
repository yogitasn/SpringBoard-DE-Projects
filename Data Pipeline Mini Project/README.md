## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)
* [Execution](#execution)
* [Output](#output)

## General Info
This project is Data pipeline mini project.

## Description
A basic pipeline created using Python and SQL skills. Functions are developed to connect to the MYSQL database, create tables, and load third-party Sale tickets data. A query is used to fetch the records programmatically for the most top-selling tickets for the past month and finally displayed in a user-friendly format.


## Technologies
Project is created with:
* Python
* MySQL Database
* mysql-connector-python


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

Using Python 3.7+, run `pip3 install mysql-connector-python` to install the MySQL Python adapter.

Navigate to project folder and execute the following commands

```
python ticket_sales.py

```

## Output

* Most Popular Tickets in the past month:

![Alt text](popular_tickets.PNG?raw=true "Popular Tickets")
