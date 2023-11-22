# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit


#Schema creation

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True) ])


# Initial DF load

df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_container_path}/{v_file_date}/circuits.csv")


# adding column "time_stamp"

df = ingestion_date(df)


#Selecting only needed columns
df = df.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))


#renaming columns (other option than using 'alias' above)

df = df \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))
    

#load to ADSL 'processd' container

df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")



# COMMAND ----------

dbutils.notebook.exit("Success")
