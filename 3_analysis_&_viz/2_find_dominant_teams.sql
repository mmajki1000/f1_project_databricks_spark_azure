-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

select 
team
,sum(points) as total_points
from f1_presentation.race_results
where race_year = '2018'
group by team

-- COMMAND ----------

--dominant teams all time where #races is greater than 50

create TEMP VIEW dominant_teams
as
select 
team
,COUNT(1) as total_races
,SUM(points) as total_points
,AVG(points) as avg_points
,RANK() OVER(ORDER BY AVG(points) DESC) team_rank
FROM f1_presentation.race_results
group by team
having COUNT(1) > 100
order by avg_points desc

-- COMMAND ----------


-- takes data from dominant drivers view for top 10 teams
SELECT
   race_year
  ,team
  ,COUNT(1) as total_races
  ,SUM(points) as total_points
  ,AVG(points) as avg_points
FROM f1_presentation.race_results
WHERE team IN (SELECT team FROM dominant_teams where team_rank <= 10)
group by race_year, team
order by race_year, avg_points desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #header for dashboard
-- MAGIC
-- MAGIC html = """<h1 style="color:Black;
-- MAGIC                   text-align:center;
-- MAGIC                   font-family:Arial;">
-- MAGIC         Dominant teams Formula 1 </h1>"""
-- MAGIC displayHTML(html)
-- MAGIC
-- MAGIC
