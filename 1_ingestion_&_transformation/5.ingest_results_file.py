# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, FloatType, StructField, StructType
from pyspark.sql.functions import col, current_timestamp, lit

results_schema = StructType(fields=[
                            StructField("raceId", IntegerType(), False), \
                            StructField("driverId", IntegerType(), False), \
                            StructField("constructorId", IntegerType(), False), \
                            StructField("number", IntegerType(), True), \
                            StructField("grid", IntegerType(), False), \
                            StructField("position", IntegerType(), True), \
                            StructField("positionText", StringType(), False), \
                            StructField("positionOrder", IntegerType(), False), \
                            StructField("points", FloatType(), False), \
                            StructField("laps", IntegerType(), False), \
                            StructField("time", StringType(), True), \
                            StructField("milliseconds", IntegerType(), True), \
                            StructField("fastestLap", IntegerType(), True), \
                            StructField("rank", IntegerType(), True), \
                            StructField("fastestLapTime", StringType(), True), \
                            StructField("fastestLapSpeed", StringType(), True), \
                            StructField("statusId", IntegerType(), False)
    ])

#initial DF load

df = spark.read \
            .schema(results_schema) \
            .json(f"{raw_container_path}/{v_file_date}/results.json")


#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("positionText", "position_text") \
        .withColumnRenamed("positionOrder", "position_order") \
        .withColumnRenamed("fastestLap", "fastest_lap") \
        .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

df = ingestion_date(df)

#drop 'statusId' column

df = df.drop("statusId")

#load into DL 'processed' container, partition by race_id

overwritePartition(df, "f1_processed", "results", "race_id")





# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------


