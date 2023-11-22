# Databricks notebook source
# function for creating and addingg ingestion date

from pyspark.sql.functions import current_timestamp

def ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df
    

# COMMAND ----------

#function to set partition column as the last in a table - needed for incremental load method

def moveColumn(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df
    


# COMMAND ----------

#function make incremental load

def overwritePartition(input_df, db_name, table_name, partition_column):
    output_df = moveColumn(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(f'{partition_column}').format("parquet").saveAsTable(f"{db_name}.{table_name}")
    return output_df
