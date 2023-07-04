#importing SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

#creating a spark session
spark = SparkSession \
.builder \
.appName("My PySpark code") \
.getOrCreate()

#reading the csv file from the bucket
dataframe=spark.read.options(header='true', inferSchema='true').csv("gs://us-central1-my-project-dcdd3083-bucket/data/daysdata.csv")

#Replacing the value 'Unspecified' in contributing factor's column with some other value
dataframe=dataframe.withColumn('contributing_factor_vehicle_1', regexp_replace('contributing_factor_vehicle_1','Unspecified','Not Available'))

#converting the the crash_date's column type from datetime to string
dataframe=dataframe.withColumn("crash_date",dataframe["crash_date"].cast(StringType()))

#for storing the date's value in a variable, in order to write the output properly
date=dataframe.collect()[0][1]

#creating a temporary view of the dataframe to work with
dataframe.printSchema()
dataframe.createOrReplaceTempView("nyctable")

#the spark sql code for processing the fetched data
results=spark.sql("""
	WITH common_borough AS(
	SELECT borough, crash_date, count(borough)
	FROM nyctable
	GROUP BY crash_date, borough ORDER BY count(borough) DESC LIMIT 1 
	)
	, common_factor AS(
	SELECT contributing_factor_vehicle_1 AS factor, crash_date, count(contributing_factor_vehicle_1)
	FROM nyctable
	GROUP BY crash_date, contributing_factor_vehicle_1 ORDER BY count(contributing_factor_vehicle_1) DESC LIMIT 1
	)
	, sum_of_people AS(
	SELECT sum(number_of_persons_injured) AS People_Injured, sum(number_of_persons_killed) AS People_Killed, crash_date
	FROM nyctable
	GROUP BY crash_date
	)
	SELECT a.crash_date AS Date_of_Crash,
		a.borough AS Common_Borough,
		b.factor AS Common_Factor,
		c.People_Injured AS People_Injured,
		c.People_Killed AS People_Killed
	FROM common_borough a 
	INNER JOIN common_factor b
		ON a.crash_date=b.crash_date
	INNER JOIN sum_of_people c
		ON b.crash_date=c.crash_date
    """)

#writing the results back to the cloud storage
results.write.csv("gs://us-central1-my-project-dcdd3083-bucket/data/"+ date +"summary.csv")