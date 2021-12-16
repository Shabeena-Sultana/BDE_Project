create database taxi_data;

use taxi_data;

CREATE EXTERNAL TABLE IF NOT EXISTS taxi_data.yellow_taxi(vendor_name STRING, Trip_Pickup_DateTime STRING, Trip_Dropoff_DateTime STRING,Passenger_Count STRING, 
Trip_Distance STRING, Start_Lon STRING, Start_Lat STRING,Rate_Code STRING, store_and_forward STRING, End_Lon STRING, End_Lat STRING, Payment_Type STRING,
Fare_Amt STRING, surcharge STRING, mta_tax STRING, Tip_Amt STRING, Tolls_Amt STRING,Total_Amt STRING,id STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/yellow_taxi';

select * from taxi_data.yellow_taxi limit 10;