drop database if exists craiglist;
create database if not exists craiglist;
use craiglist;

CREATE TABLE IF NOT EXISTS USER(
userID INT PRIMARY KEY NOT NULL,
first_name VARCHAR(20),
last_name VARCHAR(20),
preferred_region VARCHAR(25));

CREATE TABLE IF NOT EXISTS CATEGORY(
category_id INT PRIMARY KEY NOT NULL,
category VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS region(
region_id INT PRIMARY KEY NOT NULL,
region VARCHAR(30));

CREATE TABLE IF NOT EXISTS POSTS
(postId INT PRIMARY KEY NOT NULL,
title VARCHAR(25),
text VARCHAR(100),
userID INT,
location VARCHAR(30),
region_id INT,
category_id INT,
FOREIGN KEY (userID) REFERENCES USER(userID),
FOREIGN KEY(region_id) REFERENCES region(region_id),
FOREIGN KEY(category_id) REFERENCES category(category_id));

