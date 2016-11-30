CREATE DATABASE taxi_test;

USE taxi_test;

CREATE TABLE trajectories (
  id int not null auto_increment PRIMARY KEY,
  id_taxi int,
  when_ VARCHAR(255),
  latitude VARCHAR(255),
  longitude VARCHAR(255)
);