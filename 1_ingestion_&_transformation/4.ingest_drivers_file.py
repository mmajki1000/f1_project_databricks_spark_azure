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

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import lit, concat, current_timestamp, col

#implement schema

name_schema = StructType(fields=[
                        StructField("forename", StringType(), True), \
                        StructField("surname", StringType(), True)
    ])

drivers_schema  = StructType(fields=[
                        StructField("driverId", IntegerType(), False), \
                        StructField("driverRef", StringType(), True), \
                        StructField("number", IntegerType(), True), \
                        StructField("code", StringType(), True), \
                        StructField("dob", IntegerType(), True), \
                        StructField("name", name_schema), \
                        StructField("nationality", StringType(), True), \
                        StructField("url", StringType(), True)
    ])

#initial DF load

df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_container_path}/{v_file_date}/drivers.json")

#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("driverId", "drivers_id") \
    .withColumnRenamed("driverRef", "drivers_ref") \
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
    .withColumnRenamed("data_source", v_data_source) \
    .withColumnRenamed("file_date", v_file_date)
    

df = ingestion_date(df)


#droping url column
df = df.drop("url")

#loading into DL 'processed' container
df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")



# COMMAND ----------

dbutils.notebook.exit("Success")
