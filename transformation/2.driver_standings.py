# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Generate Driver Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

driver_standings_df=race_results_df \
.groupBy("race_year","driver_nationality","driver_name","team")\
.agg(sum("points").alias("total_points"),
    count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window

driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------


