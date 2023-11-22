# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

#schema creation

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False), \
                                StructField("year", IntegerType(), True), \
                                StructField("round", IntegerType(), True), \
                                StructField("circuitId", IntegerType(), True), \
                                StructField("name", StringType(), True), \
                                StructField("date", StringType(), True), \
                                StructField("time", StringType(), True), \
                                StructField("url", StringType(), True) 
                                 ])


#initial df load

df = spark.read \
        .option("Header", True) \
        .schema(race_schema) \
        .csv(f"{raw_container_path}/{v_file_date}/races.csv")


#adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),"yyyy-MM-dd HH:mm:ss")) \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

df = ingestion_date(df)



#columns selection and renaming

df = df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id") \
    ,col("name"),col("race_timestamp"),col("ingestion_date"), col("data_source"), col("file_date") )

df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")





# COMMAND ----------

dbutils.notebook.exit("Success")
