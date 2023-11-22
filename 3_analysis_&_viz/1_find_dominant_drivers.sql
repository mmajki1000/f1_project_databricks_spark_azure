-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

--driver standings in current year

select *
from f1_presentation.driver_standings
where race_year = '2020'

-- COMMAND ----------

--dominant drivers all time where #races is greater than 50

create TEMP VIEW dominant_drivers
as
select 
driver_name
,COUNT(1) as total_races
,SUM(points) as total_points
,AVG(points) as avg_points
,RANK() OVER(ORDER BY AVG(points) DESC) as driver_rank
FROM f1_presentation.race_results
group by driver_name
having COUNT(1) > 50
order by avg_points desc

-- COMMAND ----------

-- takes data from dominant drivers view for top 10 drivers

SELECT
   race_year
  ,driver_name
  ,COUNT(1) as total_races
  ,SUM(points) as total_points
  ,AVG(points) as avg_points
FROM f1_presentation.race_results
WHERE driver_name IN (SELECT driver_name FROM dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
order by race_year, avg_points desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # header for dashboard
-- MAGIC
-- MAGIC html = """<h1 style="color:Black;
-- MAGIC                 text-align:center;
-- MAGIC                 font-family:Arial;">
-- MAGIC         Dominant drivers in Formula 1 </h1>"""
-- MAGIC displayHTML(html)
-- MAGIC
-- MAGIC
-- MAGIC
