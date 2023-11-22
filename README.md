Project made to get experience in using Databricks and Spark in Azure env.
Description below: 

Table of Context:
1. Project Description & Requirements
2. Services & Tools
3. Solution Architecture


1. Project for analyzing Formula 1 data to get insights about drivers & teams performance
	
	Project Requirements:
	Technical:
	Ingestion Reqirements:
	- Ingest All 8 files into the data lake
	- Ingested data must have the schema applied
	- Ingested data must have audit columns
	- Ingest data must be stored in columnar format (i.e Parquet)
	- Must be able to analyze the ingested data via SQL
	- Ingestion logic must be able to handle incremental load
	
	Data Transormation Requirements
	- Join the key information required for reporting to create a new table
	- Join the key information required for analysis to create a new table
	- Transformed tables must have audit columns
	- Must be able to analyze the ingested data via SQL
	- Transormation logic must be able to handle incremental load

	Reporting Requirements
	- Driver Standings
	- Constructor Standings

	Analysis Requirements
	- Dominant Drivers
	- Dominant Teams
	- Visualize the outputs
	- Create Databricks Dashboards

	Non functional:
	Scheduling Requirements
	- Scheduled to run every Sunday 10PM
	- Ability to monitor pipelines
	- Ability to re-run failed pipelines
	- Ability to set-up alerts on failures

2. Azure:
	- Databricks (PySpark, SQL)
	- ADF (Pipeline)
	- Security (KeyVault, Service Principal)
	- Storage (DataLake Gen2)


