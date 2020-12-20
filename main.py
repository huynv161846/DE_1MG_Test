from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()  

schema = StructType() \
      .add("VendorID",IntegerType(),True) \
      .add("lpep_pickup_datetime",TimestampType(),True) \
      .add("Lpep_dropoff_datetime",TimestampType(),True) \
      .add("Store_and_fwd_flag",StringType(),True) \
      .add("RateCodeID",IntegerType(),True) \
      .add("Pickup_longitude",DoubleType(),True) \
      .add("Pickup_latitude",DoubleType(),True) \
      .add("Dropoff_longitude",DoubleType(),True) \
      .add("Dropoff_latitude",DoubleType(),True) \
      .add("Passenger_count",IntegerType(),True) \
      .add("Trip_distance",DoubleType(),True) \
      .add("Fare_amount",DoubleType(),True) \
      .add("Extra",DoubleType(),True) \
      .add("MTA_tax",DoubleType(),True) \
      .add("Tip_amount",DoubleType(),True) \
      .add("Tolls_amount",DoubleType(),True) \
      .add("Ehail_fee",DoubleType(),True) \
      .add("Total_amount",DoubleType(),True) \
      .add("Payment_type",IntegerType(),True) \
      .add("Trip_type",IntegerType(),True)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').schema(schema).csv('green_tripdata_2013-09.csv')
df.createOrReplaceTempView('data')

df2 = spark.sql("select VendorID, lpep_pickup_datetime, Lpep_dropoff_datetime, \
                 Store_and_fwd_flag, RateCodeID, Pickup_longitude, Pickup_latitude, \
                 Dropoff_longitude, Dropoff_latitude, Passenger_count, Trip_distance, \
                 Fare_amount, Extra, MTA_tax, Tip_amount, Tolls_amount, Ehail_fee, \
                 Total_amount, Payment_type, Trip_type, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 0 then 1 end) as lpep_pickup_hour_0, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 1 then 1 end) as lpep_pickup_hour_1, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 2 then 1 end) as lpep_pickup_hour_2, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 3 then 1 end) as lpep_pickup_hour_3, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 4 then 1 end) as lpep_pickup_hour_4, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 5 then 1 end) as lpep_pickup_hour_5, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 6 then 1 end) as lpep_pickup_hour_6, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 7 then 1 end) as lpep_pickup_hour_7, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 8 then 1 end) as lpep_pickup_hour_8, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 9 then 1 end) as lpep_pickup_hour_9, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 10 then 1 end) as lpep_pickup_hour_10, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 11 then 1 end) as lpep_pickup_hour_11, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 12 then 1 end) as lpep_pickup_hour_12, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 13 then 1 end) as lpep_pickup_hour_13, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 14 then 1 end) as lpep_pickup_hour_14, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 15 then 1 end) as lpep_pickup_hour_15, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 16 then 1 end) as lpep_pickup_hour_16, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 17 then 1 end) as lpep_pickup_hour_17, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 18 then 1 end) as lpep_pickup_hour_18, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 19 then 1 end) as lpep_pickup_hour_19, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 20 then 1 end) as lpep_pickup_hour_20, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 21 then 1 end) as lpep_pickup_hour_21, \
                 count(case when EXTRACT(hour from lpep_pickup_datetime) = 22 then 1 end) as lpep_pickup_hour_22, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 23 then 1 end) as lpep_pickup_hour_23, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 0 then 1 end) as lpep_dropoff_hour_0, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 1 then 1 end) as lpep_dropoff_hour_1, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 2 then 1 end) as lpep_dropoff_hour_2, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 3 then 1 end) as lpep_dropoff_hour_3, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 4 then 1 end) as lpep_dropoff_hour_4, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 5 then 1 end) as lpep_dropoff_hour_5, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 6 then 1 end) as lpep_dropoff_hour_6, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 7 then 1 end) as lpep_dropoff_hour_7, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 8 then 1 end) as lpep_dropoff_hour_8, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 9 then 1 end) as lpep_dropoff_hour_9, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 10 then 1 end) as lpep_dropoff_hour_10, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 11 then 1 end) as lpep_dropoff_hour_11, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 12 then 1 end) as lpep_dropoff_hour_12, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 13 then 1 end) as lpep_dropoff_hour_13, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 14 then 1 end) as lpep_dropoff_hour_14, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 15 then 1 end) as lpep_dropoff_hour_15, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 16 then 1 end) as lpep_dropoff_hour_16, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 17 then 1 end) as lpep_dropoff_hour_17, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 18 then 1 end) as lpep_dropoff_hour_18, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 19 then 1 end) as lpep_dropoff_hour_19, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 20 then 1 end) as lpep_dropoff_hour_20, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 21 then 1 end) as lpep_dropoff_hour_21, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 22 then 1 end) as lpep_dropoff_hour_22, \
                 count(case when EXTRACT(hour from Lpep_dropoff_datetime) = 23 then 1 end) as lpep_dropoff_hour_23, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 2 then 1 end) as lpep_pickup_monday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 3 then 1 end) as lpep_pickup_tuesday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 4 then 1 end) as lpep_pickup_wednesday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 5 then 1 end) as lpep_pickup_thursday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 6 then 1 end) as lpep_pickup_friday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 7 then 1 end) as lpep_pickup_saturday, \
                 count(case when DAYOFWEEK(lpep_pickup_datetime) = 1 then 1 end) as lpep_pickup_sunday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 2 then 1 end) as lpep_dropoff_monday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 3 then 1 end) as lpep_dropoff_tuesday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 4 then 1 end) as lpep_dropoff_wednesday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 5 then 1 end) as lpep_dropoff_thursday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 6 then 1 end) as lpep_dropoff_friday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 7 then 1 end) as lpep_dropoff_saturday, \
                 count(case when DAYOFWEEK(Lpep_dropoff_datetime) = 1 then 1 end) as lpep_dropoff_sunday, \
                 (unix_timestamp(Lpep_dropoff_datetime)-unix_timestamp(lpep_pickup_datetime)) as trip_duration, \
                 case when ((Pickup_longitude < -73.7554364 and Pickup_longitude > -73.8008418 and Pickup_latitude < 40.6640138 and Pickup_latitude > 40.6186084) \
                 or (Dropoff_longitude < -73.7554364 and Dropoff_longitude > -73.8008418 and Dropoff_latitude < 40.6640138 and Dropoff_latitude > 40.6186084)) then 1 else 0 end as at_jfk\
                 FROM data GROUP BY VendorID, lpep_pickup_datetime, Lpep_dropoff_datetime, \
                 Store_and_fwd_flag, RateCodeID, Pickup_longitude, Pickup_latitude, \
                 Dropoff_longitude, Dropoff_latitude, Passenger_count, Trip_distance, \
                 Fare_amount, Extra, MTA_tax, Tip_amount, Tolls_amount, Ehail_fee, \
                 Total_amount, Payment_type, Trip_type")

df2.collect()

df2.repartition(1).write.parquet("output.parquet")