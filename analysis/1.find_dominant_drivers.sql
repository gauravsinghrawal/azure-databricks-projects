-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

SELECT driver_name,
        COUNT(1) AS total_races,
        SUM(calculated_points) as total_points,
        AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2020
GROUP BY driver_name
HAVING COUNT(1)>=50
ORDER BY avg_points DESC;

-- COMMAND ----------


