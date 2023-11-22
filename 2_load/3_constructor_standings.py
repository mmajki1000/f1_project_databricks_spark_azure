# Databricks notebook source
# This notebook is for calculating constructor standings over year based on a logic
# Method of doing this includes incremental load processing, so that only data based on file date parameter is processing

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#load race result table and collect distinct list of race years that there already exists
#It is needed to filter data that should be only processed based on file date parameter

race_results_list = spark.read.parquet(f"{presentation_container_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.parquet(f"{presentation_container_path}/race_results") \
    .filter(col("race_year").isin(race_year_list)) 

# COMMAND ----------

# calculations

from pyspark.sql.functions import sum, when, col,count


agg_df = df.groupBy("race_year","team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

#constructors standings creation by using rank

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = agg_df.withColumn("rank", rank().over(constructor_rank_spec))



# COMMAND ----------

# Incremental load into table, into presentation layer based on race year partition. Externally loaded function recognize if teable exists or not and proceed accordingly

overwritePartition(final_df, "f1_presentation", "constructor_standings", "race_year" )

# COMMAND ----------

dbutils.notebook.exit("Success")
