# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration/"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

pit_stops_schema = StructType(fields=[
                    StructField("raceId", IntegerType(), True), \
                    StructField("driverId", IntegerType(), True), \
                    StructField("stop", IntegerType(), True), \
                    StructField("lap", IntegerType(), True), \
                    StructField("time", StringType(), True), \
                    StructField("duration", StringType(), False), \
                    StructField("milliseconds", IntegerType(), False)

])

#initial df load of multiline .json

df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f"{raw_container_path}/{v_file_date}/pit_stops.json") 


#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("raceId","race_id") \
        .withColumnRenamed("driverId","driver_id") \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

df = ingestion_date(df)

# load into DL 'processed' container

overwritePartition(df, "f1_processed", "pit_stops", "race_id" )



# COMMAND ----------

dbutils.notebook.exit("Success")
