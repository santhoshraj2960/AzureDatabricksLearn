# Databricks notebook source
# MAGIC %fs ls /mnt/storage

# COMMAND ----------

# Reading without specifying the schema takes long time

fhv_trips_df = spark \
              .read \
              .option('header', 'true') \
              .option('inferSchema', 'true') \
              .csv('/mnt/storage/fhv_tripdata_2018-12.csv')

# COMMAND ----------

# Create schema for FHV taxi data

from pyspark.sql.types import *

fhv_taxi_trips_schema = StructType([
    StructField("Pickup_DateTime", TimestampType(), True),
    StructField("DropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", IntegerType(), True),
    StructField("DOlocationID", IntegerType(), True),
    StructField("SR_Flag", IntegerType(), True),
    StructField("Dispatching_base_number", StringType(), True),
    StructField("Dispatching_base_num", StringType(), True)
])

'''
val fhvTaxiTripSchema = StructType(
                    List(
                          StructField("Pickup_DateTime", TimestampType, true),
                          StructField("DropOff_datetime", TimestampType, true),
                          StructField("PUlocationID", IntegerType, true),
                          StructField("DOlocationID", IntegerType, true),
                          StructField("SR_Flag", IntegerType, true),
                          StructField("Dispatching_base_number", StringType, true),
                          StructField("Dispatching_base_num", StringType, true)
                    )
             )
'''

# COMMAND ----------

fhv_trips_df = spark \
              .read \
              .schema(fhv_taxi_trips_schema) \
              .csv('/mnt/storage/fhv_tripdata_2018-12.csv')