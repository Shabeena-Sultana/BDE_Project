create database taxi_data;
create table taxi_data.yellow_taxi(vendor_name varchar(20), Trip_Pickup_DateTime varchar(30), Trip_Dropoff_DateTime varchar(30),Passenger_Count int,Trip_Distance varchar(10), Start_Lon varchar(30), Start_Lat varchar(30),Rate_Code varchar(20), store_and_forward varchar(30), End_Lon varchar(30), End_Lat varchar(30), Payment_Type varchar(20),Fare_Amt varchar(20), surcharge varchar(20), mta_tax varchar(20), Tip_Amt varchar(20), Tolls_Amt varchar(20),Total_Amt varchar(20));
LOAD DATA INFILE '/home/hadoop/yellow_taxi_data.csv' INTO TABLE taxi_data.yellow_taxi COLUMNS TERMINATED BY ',';
ALTER TABLE taxi_data.yellow_taxi ADD record_id INT PRIMARY KEY AUTO_INCREMENT;
