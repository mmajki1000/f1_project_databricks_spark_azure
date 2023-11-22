# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#below data loadings for further join

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_container_path}/drivers') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')



# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_container_path}/circuits') \
    .withColumnRenamed('location', 'circuit_location')



# COMMAND ----------

races_df = spark.read.parquet(f'{processed_container_path}/races') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('name','race_name') \
    .withColumnRenamed('race_timestamp','race_date')


# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_container_path}/constructors') \
    .withColumnRenamed('name','team')



# COMMAND ----------

results_df = spark.read.parquet(f'{processed_container_path}/results') \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed('time','race_time') \
    .withColumnRenamed('race_id','result_race_id') \
    .withColumnRenamed('file_date','result_file_date')




# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner' ) 



# COMMAND ----------

#joining loaded dfs

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, drivers_df.drivers_id == results_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


# COMMAND ----------

#after join selecting oly needed columns

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team",
                                  "grid", "fastest_lap", "points", "position","result_file_date") \
                          .withColumnRenamed("result_file_date", "file_date")

final_df = ingestion_date(final_df)

# COMMAND ----------

# load created table into presentation container using function

overwritePartition(final_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
