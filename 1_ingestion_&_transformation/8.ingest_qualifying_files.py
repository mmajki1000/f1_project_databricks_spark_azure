# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_file_date = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration/"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

qualifying_schema = StructType(fields= [
                                StructField("qualifyId", IntegerType(), False), \
                                StructField("raceId", IntegerType(), False), \
                                StructField("driverId", IntegerType(), False), \
                                StructField("constructorId", IntegerType(), False), \
                                StructField("number", IntegerType(), False), \
                                StructField("position", IntegerType(), True), \
                                StructField("q1", StringType(), True), \
                                StructField("q2", StringType(), True), \
                                StructField("q3", StringType(), True)
                                
])



#initial DF load from folder

df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_container_path}/{v_file_date}/qualifying/qualifying_split*")


#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("qualifyId", "qualify_id") \
        .withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("constructorId", "constructor_id") \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))


df = ingestion_date(df)

#loading into ADLS 'processed' container

overwritePartition(df, "f1_processed", "qualifying", "race_id" )




# COMMAND ----------

dbutils.notebook.exit("Success")
