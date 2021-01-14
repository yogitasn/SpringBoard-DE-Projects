## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [DataModel](#datamodel)
* [Setup](#setup)
* [Execution](#execution)
* [References](#references)

## General Info
This project is python bank application using OOP mini project.

## Description
Flask-SQLAlchemy is the Flask extension that adds support for SQLAlchemy. Most programming language platforms are object oriented. Data in RDBMS servers on the other hand is stored as tables. Object relation mapping is a technique of mapping object parameters to the underlying RDBMS table structure. An ORM API provides methods to perform CRUD operations without having to write raw SQL statements.


Project is created using Flask-SQLALchemy and Flask with MYSQL as Database for storing data. Models are created for Customer, Employees, BankAccount and CreditCard. All the tables are populated using flask-sqlalchemy library functions. User is prompted to enter the Customer ID and option to deposit and withdraw from the Bank Account


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
