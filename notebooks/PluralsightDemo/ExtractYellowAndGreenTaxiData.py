# Databricks notebook source
yellow_taxi_trips_df = spark \
                      .read \
                      .option('inferSchema', 'true') \
                      .option('header', 'true') \
                      .csv('/mnt/datalake/yellow_tripdata_2018-12.csv')

# COMMAND ----------

yellow_taxi_trips_df.count()

# COMMAND ----------

# MAGIC %md ###Creating and writing dataframe to a new unmanaged delta table to optimize processing time

# COMMAND ----------

yellow_taxi_trips_df \
.write \
.format("delta") \
.mode("overwrite") \
.save("/mnt/datalake/yellow_taxi_trips")

# COMMAND ----------

yellow_taxi_trips_df = spark.read.format("delta").load("/mnt/datalake/yellow_taxi_trips")

# COMMAND ----------

yellow_taxi_trips_df.count()

# COMMAND ----------

display(
  yellow_taxi_trips_df.describe(
  "passenger_count",
  "trip_distance")
)

# COMMAND ----------

print('Before filter ', yellow_taxi_trips_df.count())

yellow_taxi_trips_df = yellow_taxi_trips_df \
                        .filter(
                               ("passenger_count > 0") and ("trip_distance > 0")
                          )

yellow_taxi_trips_df = yellow_taxi_trips_df \
                      .filter( \
                        (yellow_taxi_trips_df["passenger_count"] > 0) & (yellow_taxi_trips_df["trip_distance"] > 0) \
                        )

yellow_taxi_trips_df = yellow_taxi_trips_df \
                        .filter(
                               ("passenger_count > 0 AND trip_distance > 0")
                          )

print('after filter ', yellow_taxi_trips_df.count())

# COMMAND ----------

# MAGIC %md ###The keyword 'where' and 'filter' can be used interchangably. So, all the following statements are also valid
# MAGIC 
# MAGIC - yellow_taxi_trips_df = yellow_taxi_trips_df \
# MAGIC                         .where(
# MAGIC                                ("passenger_count > 0") and ("trip_distance > 0")
# MAGIC                           )
# MAGIC 
# MAGIC - yellow_taxi_trips_df = yellow_taxi_trips_df \
# MAGIC                       .where( \
# MAGIC                         (yellow_taxi_trips_df["passenger_count"] > 0) & (yellow_taxi_trips_df["trip_distance"] > 0) \
# MAGIC                         )
# MAGIC 
# MAGIC - yellow_taxi_trips_df = yellow_taxi_trips_df \
# MAGIC                         .where(
# MAGIC                                ("passenger_count > 0 AND trip_distance > 0")
# MAGIC                           )
# MAGIC 
# MAGIC - yellowTaxiTripDataDF = yellowTaxiTripDataDF
# MAGIC                           .where("passenger_count > 0")
# MAGIC                           .filter($"trip_distance" > 0.0)
# MAGIC                           
# MAGIC You can refer to columns in all the below ways
# MAGIC 
# MAGIC - "passenger_count > 0"
# MAGIC 
# MAGIC - $"passenger_count" > 0
# MAGIC 
# MAGIC - col("passenger_count") > 0
# MAGIC 
# MAGIC - yellow_taxi_trips_df["passenger_count"] > 0

# COMMAND ----------

print('Before filter ', yellow_taxi_trips_df.count())

yellow_taxi_trips_df = yellow_taxi_trips_df \
                        .dropna(
                                subset=["PULocationID", "DOLocationID"]
                            )

print('After filter ', yellow_taxi_trips_df.count())

# COMMAND ----------

# Rate code id of yellow and green taxi refers to if the trip is a solo, shared or trip to any specific airport
# But Fhv trips data contains only 2 values, 0 and 1 (or flag) to tell us if it's a solo trip or a shared trip
# Since our goal is to merge yellow, green and fhv trips data. We convert the missing values of RateCode to 1
# See this video to understand more about data https://app.pluralsight.com/course-player?clipId=911371e0-0d10-4470-a688-9852e9440c94

display(
  yellow_taxi_trips_df.describe(
  "payment_type",
  "RatecodeID")
)

# COMMAND ----------

default_vals_dict = {
  "payment_type": 5,
  "RatecodeID": 1
}

# COMMAND ----------

yellow_taxi_trips_df = yellow_taxi_trips_df.fillna(default_vals_dict)

# COMMAND ----------

print('Before filter ', yellow_taxi_trips_df.count())

yellow_taxi_trips_df = yellow_taxi_trips_df.drop_duplicates() \

print('After filter ', yellow_taxi_trips_df.count())

# COMMAND ----------

'''

print('Before filter ', yellow_taxi_trips_df.count())

yellow_taxi_trips_df = yellow_taxi_trips_df \
                        .filter(
                                "tpep_pickup_datetime >= 2018-12-01 AND tpep_dropoff_datetime <= 2018-12-31" 
                            )

print('After filter ', yellow_taxi_trips_df.count())
'''

# COMMAND ----------

# MAGIC %md ## Important thing to note in the above command about date range
# MAGIC 
# MAGIC - The above command will NOT properly filter the date range. The 'After filter ' count was 0. 
# MAGIC - Rather you should use single quote to enclose the date as in the below command

# COMMAND ----------

default_vals_dict = {
  "payment_type": 5,
  "RatecodeID": 1
}

print('Before filter ', yellow_taxi_trips_df.count())

yellow_taxi_trips_df = yellow_taxi_trips_df \
                        .filter(("passenger_count > 0 AND trip_distance > 0")) \
                        \
                        .dropna(subset=["PULocationID", "DOLocationID"]) \
                        \
                        .fillna(default_vals_dict) \
                        \
                        .drop_duplicates() \
                        \
                        .filter("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime <= '2018-12-31'" ) 

print('After filter ', yellow_taxi_trips_df.count())

# COMMAND ----------

