# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration/"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

#DDL schema

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

#load from folder

df = spark.read \
            .schema(constructors_schema) \
            .json(f"{raw_container_path}/{v_file_date}/constructors.json")


#renaming columns + adding new: 'data_source' + 'file_date' + 'ingestion_date'

df = df.withColumnRenamed("constructorId","constructor_id") \
        .withColumnRenamed("constructorRef","constructor_ref") \
        .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

df = ingestion_date(df)

# drop 'url' column
  
df = df.drop("url")

#write DF into DL container 'processed'

df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")


# COMMAND ----------

dbutils.notebook.exit("Success")
