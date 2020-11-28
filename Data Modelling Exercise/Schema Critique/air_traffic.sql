-- from the terminal run:
-- psql < air_traffic.sql

DROP DATABASE IF EXISTS air_traffic;

CREATE DATABASE air_traffic;

USE air_traffic;
CREATE TABLE passenger
(
passenger_id SERIAL PRIMARY KEY,
first_name TEXT NOT NULL,
last_name TEXT NOT NULL
);

CREATE TABLE airline
(
airline_id SERIAL PRIMARY KEY,
airline TEXT NOT NULL
);

CREATE TABLE tickets
(
  id SERIAL PRIMARY KEY,
  passenger_id BIGINT UNSIGNED,
  seat TEXT NOT NULL,
  departure TIMESTAMP NOT NULL,
  arrival TIMESTAMP NOT NULL,
  airline_id BIGINT UNSIGNED,
  from_city TEXT NOT NULL,
  from_country TEXT NOT NULL,
  to_city TEXT NOT NULL,
  to_country TEXT NOT NULL,
  FOREIGN KEY(passenger_id) REFERENCES passenger(passenger_id),
  FOREIGN KEY(airline_id) REFERENCES airline(airline_id)
);
INSERT INTO passenger 
  (first_name,last_name)
  VALUES
  ('Jennifer', 'Finch'),
  ('Thadeus', 'Gathercoal'),
  ('Sonja', 'Pauley'),
  ('Waneta', 'Skeleton'),
  ('Berkie', 'Wycliff'),
  ('Alvin', 'Leathes'),
  ('Cory', 'Squibbes');
  
  INSERT INTO airline
  (airline)
  VALUES
  ('United'),
  ('British Airways'),
  ('Delta'),
  ('TUI Fly Belgium'),
  ('Air China'),
  ('American Airlines'),
  ('Avianca Brasil');
  
INSERT INTO tickets
  (passenger_id,seat, departure, arrival,airline_id,from_city, from_country, to_city, to_country)
VALUES
  ((select passenger_id from passenger where first_name = 'Jennifer'),'33B', '2018-04-08 09:00:00', '2018-04-08 12:00:00', (select airline_id from airline where airline='United'),'Washington DC', 'United States', 'Seattle', 'United States'),
  ((select passenger_id from passenger where first_name = 'Thadeus'),'8A', '2018-12-19 12:45:00', '2018-12-19 16:15:00',(select airline_id from airline where airline='British Airways'), 'Tokyo', 'Japan', 'London', 'United Kingdom'),
  ((select passenger_id from passenger where first_name = 'Sonja'),'12F', '2018-01-02 07:00:00', '2018-01-02 08:03:00', (select airline_id from airline where airline='Delta'),'Los Angeles', 'United States', 'Las Vegas', 'United States'),
  ((select passenger_id from passenger where first_name = 'Jennifer'),'20A', '2018-04-15 16:50:00', '2018-04-15 21:00:00',(select airline_id from airline where airline='Delta'),'Seattle', 'United States', 'Mexico City', 'Mexico'),
  ((select passenger_id from passenger where first_name = 'Waneta'),'23D', '2018-08-01 18:30:00', '2018-08-01 21:50:00',(select airline_id from airline where airline='TUI Fly Belgium'),'Paris', 'France', 'Casablanca', 'Morocco'),
  ((select passenger_id from passenger where first_name = 'Thadeus'),'18C', '2018-10-31 01:15:00', '2018-10-31 12:55:00',(select airline_id from airline where airline='Air China'),'Dubai', 'UAE', 'Beijing', 'China'),
  ((select passenger_id from passenger where first_name = 'Berkie'),'9E', '2019-02-06 06:00:00', '2019-02-06 07:47:00', (select airline_id from airline where airline='United'),'New York', 'United States', 'Charlotte', 'United States'),
  ((select passenger_id from passenger where first_name = 'Alvin'),'1A', '2018-12-22 14:42:00', '2018-12-22 15:56:00', (select airline_id from airline where airline='American Airlines'),'Cedar Rapids', 'United States', 'Chicago', 'United States'),
  ((select passenger_id from passenger where first_name = 'Berkie'),'32B', '2019-02-06 16:28:00', '2019-02-06 19:18:00',(select airline_id from airline where airline='American Airlines'),'Charlotte', 'United States', 'New Orleans', 'United States'),
  ((select passenger_id from passenger where first_name = 'Cory'),'10D', '2019-01-20 19:30:00', '2019-01-20 22:45:00', (select airline_id from airline where airline='Avianca Brasil'),'Sao Paolo', 'Brazil', 'Santiago', 'Chile');
    
  select * from tickets;
  select * from passenger;
  select * from airline;