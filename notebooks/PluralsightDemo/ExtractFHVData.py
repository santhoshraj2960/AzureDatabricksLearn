# Databricks notebook source
# MAGIC %fs ls /mnt/storage

# COMMAND ----------

# Reading without specifying the schema takes long time


fhv_trips_df = spark \
              .read \
              .option('header', 'true') \
              .option('inferSchema', 'true') \
              .csv('/mnt/storage/fhv_tripdata_2018*.csv')# Read multiple files of FHV taxi data

# COMMAND ----------

# Create schema for FHV taxi data

from pyspark.sql.types import *

# Define schema for columns of Fhv trips csv file

fhv_taxi_trips_schema = StructType([
    StructField("Pickup_DateTime", TimestampType(), True),
    StructField("DropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", IntegerType(), True),
    StructField("DOlocationID", IntegerType(), True),
    StructField("SR_Flag", IntegerType(), True),
    StructField("Dispatching_base_number", StringType(), True),
    StructField("Dispatching_base_num", StringType(), True)
])

# COMMAND ----------

# Apply schema to FHV taxi data

fhv_trips_df = spark \
              .read \
              .schema(fhv_taxi_trips_schema) \
              .csv('/mnt/storage/fhv_tripdata_2018-12.csv')

# COMMAND ----------

# MAGIC %fs head /mnt/storage/FhvBases.json

# COMMAND ----------

fhv_bases_df = spark \
                .read \
                .option('multiline', 'true') \
                .json('/mnt/storage/FhvBases.json')

# COMMAND ----------

from pyspark.sql.types import *

# Defining a complex shema ("Address" is a complex structure)

fhv_bases_schema = StructType(
  [
    StructField("License Number", StringType(), True),
    StructField("Entity Name", StringType(), True),
    StructField("Telephone Number", LongType(), True),
    StructField("SHL Endorsed", StringType(), True),
    StructField("Type of Base", StringType(), True),
    
    StructField("Address", 
                StructType([
                    StructField("Building", StringType(), True),
                    StructField("Street", StringType(), True), 
                    StructField("City", StringType(), True), 
                    StructField("State", StringType(), True), 
                    StructField("Postcode", StringType(), True)
                ]),
                True
                ),
                
    StructField("GeoLocation", 
                StructType([
                    StructField("Latitude", StringType(), True),
                    StructField("Longitude", StringType(), True), 
                    StructField("Location", StringType(), True)
                ]),
                True
              )   
  ]
)

# COMMAND ----------

# Applying the schema defined above to fhv bases df
# Applying schema Will not throw an error. 
#  If any of the fields defined in schema is not present, the value will be set to null for that
#  If any additional columns are present in the json, they will be ignored

fhv_bases_df = spark \
                .read \
                .schema(fhv_bases_schema) \
                .option('multiline', 'true') \
                .json('/mnt/storage/FhvBases.json')

display(fhv_bases_df)

# COMMAND ----------

# MAGIC %md ###Creating a new unmanaged delta table and writing dataframe to it to optimize processing time

# COMMAND ----------

fhv_trips_df.write \
            .format('delta') \
            .mode('overwrite') \
            .save('/mnt/storage/fhv_trips')

# COMMAND ----------

fhv_trips_df = spark.read.format('delta').load('/mnt/storage/fhv_trips')

# COMMAND ----------

fhv_trips_df = fhv_trips_df \
                .dropna(subset=["PULocationID", "DOLocationID"]) \
                .drop_duplicates() \
                .where("Pickup_DateTime >= '2018-12-01' AND DropOff_datetime <= '2018-12-31'")

# COMMAND ----------

fhv_trips_df.count()

# COMMAND ----------

fhv_trips_df.printSchema()

# COMMAND ----------

# Removing columns that are redundant

fhv_trips_df = fhv_trips_df \
                .select(
                 "Pickup_DateTime",
                 "DropOff_datetime",
                 "PULocationID",
                 "DOLocationID",
                 "SR_Flag",
                 "Dispatching_base_number"
                  )

fhv_trips_df.printSchema()

# COMMAND ----------

# MAGIC %md Alternatively in the above command you could have done fhv_trips_df.drop("Dispatching_base_num")

# COMMAND ----------

from pyspark.sql.functions import col

fhv_trips_df = fhv_trips_df.select(
                            col("Pickup_DateTime").alias("PickupTime"), 
                            "DropOff_DateTime", 
                            "PUlocationID", 
                            "DOlocationID", 
                            "SR_Flag", 
                            "Dispatching_base_number"
                         )

fhv_trips_df.printSchema()

# COMMAND ----------

fhv_trips_df = fhv_trips_df \
                        .withColumnRenamed("DropOff_DateTime", "DropTime") \
                        .withColumnRenamed("PUlocationID", "PickupLocationId") \
                        .withColumnRenamed("DOlocationID", "DropLocationId") \
                        .withColumnRenamed("Dispatching_base_number", "BaseLicenseNumber")