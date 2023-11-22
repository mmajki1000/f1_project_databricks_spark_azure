# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")



# COMMAND ----------

# MAGIC %run "../includes/configuration/"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp,lit

lap_times_schema = StructType(fields= [
                                StructField("raceId", IntegerType(), False), \
                                StructField("driverId", IntegerType(), False), \
                                StructField("lap", IntegerType(), False), \
                                StructField("position", IntegerType(), True), \
                                StructField("time", StringType(), True), \
                                StructField("milliseconds", IntegerType(), True)
])

#initial DF load from folder with wild card

df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_container_path}/{v_file_date}/lap_times/lap_times_split*.csv")


#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date)) 

df = ingestion_date(df)

#loading into ADLS 'processed' container

overwritePartition(df, "f1_processed", "lap_times", "race_id" )




# COMMAND ----------

dbutils.notebook.exit("Success")
