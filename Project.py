from pyspark.sql.functions import *
from pyspark.sql.types import LongType,StringType
from geopy.geocoders import Nominatim
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext


spark = SparkSession.builder.appName("BDE_Project").config(conf=SparkConf().set("spark.hadoop.validateOutputSpecs", "false")).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df=spark.sql("select * from taxi_data.yellow_taxi")

def to_address(pair):
    os.system("pip install geopy")
    geolocator = Nominatim(user_agent="geoapiExercises")
    location=geolocator.reverse(pair)
    return str(location.raw['address'])

to_address_udf = udf(to_address, StringType())


# Converting start Lon and Start Lat to start address

from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
df2=df.withColumn("pair",concat(col("Start_Lat").cast("String"),lit(","),col("Start_Lon").cast("String"))) \
    .withColumn("address",to_address_udf(col("pair"))) 
df3=df2.drop("pair").withColumn("start_address",from_json(df2.address,MapType(StringType(),StringType())))

df4=df3.select(col('Trip_Pickup_DateTime'),col('Trip_Dropoff_DateTime'), \
       col('Passenger_Count'),json_tuple(to_json(col("start_address")), 'suburb'),col('surcharge')) \
       .toDF('Trip_Pickup_DateTime','Trip_Dropoff_DateTime','Passenger_Count','start_suburb', 'surcharge')


from pyspark.sql.functions import unix_timestamp, from_unixtime, date_format

df5=df4.select(from_unixtime(unix_timestamp(col('Trip_Pickup_DateTime'),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd").alias("Trip_Pickup_Date").cast("date"),\
               from_unixtime(unix_timestamp(col('Trip_Pickup_DateTime'),"yyyy-MM-dd HH:mm:ss"),"HH").alias("trip_pickup_hour"),\
               'Passenger_Count',\
               'surcharge',\
               'start_suburb')
#df5.write.option("header",True).csv("/user/hadoop/data")
df5.write.saveAsTable("taxi_data.extracted_taxi_data",mode="overwrite",path="/user/hadoop/data")