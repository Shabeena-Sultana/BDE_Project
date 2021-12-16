
from pyspark.sql.functions import *
from pyspark.sql.types import LongType,StringType
from geopy.geocoders import Nominatim
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext

#Spark Session intialization

spark = SparkSession.builder.appName("BDE_Project_analytics").config(conf=SparkConf().set("spark.hadoop.validateOutputSpecs", "false")).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Calculating the Rush Hours in each Borough
rush_hours=spark.sql("select * from (select trip_pickup_hour,borough,count(*) as trips, rank() over(partition by borough order by count(*) desc) as rnk from taxi_data.extracted_taxi_data group by trip_pickup_hour,borough) where rnk==1 and borough is not null order by trips desc").collect()
rush_hours_in_each_borough={}
for line in rush_hours:
    rush_hours_in_each_borough[line[1]]=line[0]
print(rush_hours_in_each_borough)


# Finding the type of car needed during Rush Hours in each Borough
import statistics
from statistics import mode 
type_of_car_in_each_borough={}
for borough,rush_hour in rush_hours_in_each_borough.items():
    sql_stmnt="select Passenger_Count from taxi_data.extracted_taxi_data where borough='"+borough+"' and trip_pickup_hour="+str(rush_hour)
    passenger_count_output=spark.sql(sql_stmnt).collect()
    list1 = []
    for count in passenger_count_output:
        list1.append(count[0])
    no_of_passengers=mode(list1)
    type_of_car=''
    if no_of_passengers>=4:
        type_of_car='SUV'
    else:
        type_of_car='Sedan'
    type_of_car_in_each_borough[borough]=type_of_car
print(type_of_car_in_each_borough)

# Calculating the highest surcharge the people are willing to pay in each Borough
surcharge=spark.sql("select borough,max(surcharge) as max_surcharge from taxi_data.extracted_taxi_data where borough is not null group by borough order by max(surcharge) desc ").collect()
borough_and_max_surcharge={}
for i in surcharge:
    borough_and_max_surcharge[i[0]]=i[1]

print(borough_and_max_surcharge)