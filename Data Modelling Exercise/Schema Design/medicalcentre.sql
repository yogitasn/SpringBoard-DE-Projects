drop database if exists medicalcentre;
create database medicalcentre;
use medicalcentre;
create table IF NOT EXISTS doctor
(DrCode INT PRIMARY KEY NOT NULL,
first_name VARCHAR(20),
last_name VARCHAR(20),
address VARCHAR(100),
contact VARCHAR(10),
field_of_specialization VARCHAR(20));  

CREATE TABLE IF NOT EXISTS PATIENT(
patientId INT PRIMARY KEY NOT NULL,
first_name VARCHAR(25),
last_name VARCHAR(25),
gender VARCHAR(10),
age INT,
address VARCHAR(100),
contact VARCHAR(20));

CREATE TABLE IF NOT EXISTS DIAGONSIS(
diagonsis_id INT PRIMARY KEY NOT NULL,
category VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS PRESCRIPTION(
perscription_id INT PRIMARY KEY NOT NULL,
medicine_quantity INT);

CREATE TABLE IF NOT EXISTS Visit(
visit_id INT PRIMARY KEY NOT NULL,
visit_date VARCHAR(30),
complaint VARCHAR(100),
diagonsis_id INT, 
DrCode INT,
perscription_id INT,
FOREIGN KEY (diagonsis_id) REFERENCES DIAGONSIS(diagonsis_id),
FOREIGN KEY (DrCode) REFERENCES Doctor(DrCode),
FOREIGN KEY (perscription_id) REFERENCES PRESCRIPTION(perscription_id));

CREATE TABLE IF NOT EXISTS History 
(recordId INT PRIMARY KEY NOT NULL,
patientId INT,
visit_id INT,
FOREIGN KEY(patientId) REFERENCES Patient(patientId),
FOREIGN KEY(visit_id) REFERENCES Visit(visit_id));

INSERT INTO DOCTOR 
(DrCode,first_name,last_name,address,contact,field_of_specialization)  
VALUES
(1,'John','Smith','234 King St','1-567-8889','Neurology'),
(2,'Karen','Baker','235 Upland Drive','1-567-8888','Radiology');

select * from doctor;

INSERT INTO Patient 
(patientId,first_name,last_name,gender,age,address,contact)
VALUES
(1,'Raju','Sharma','Male',25,'679 Gerald Street','1-777-8888'),
(2,'Kiran','Gupta','Female',32,'67 Tauntan Road','1-567-8888');

select * from Patient;

INSERT INTO DIAGONSIS
(diagonsis_id,category)
VALUES
(1,'Diabeties'),
(2,'Hyperthyroidism');

select * from diagonsis;

INSERT INTO PRESCRIPTION(
perscription_id,medicine_quantity)
VALUES
(1,20),
(2,10);

select * from PRESCRIPTION;
INSERT INTO Visit
(visit_id,visit_date,complaint,diagonsis_id,DrCode,perscription_id)
VALUES
(1,'2016-08-10','Headache,Weight Gain',1,1,1);

select * from Visit;

INSERT INTO History 
(recordId,patientId,visit_id)
VALUES
(1,1,1);

select * from History;
