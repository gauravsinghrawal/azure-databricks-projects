# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Exercise of Joins and Filtering for calculating race results
# MAGIC ### Step 1: Read all the required dataframes

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number","driver_number")\
.withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")\
.withColumnRenamed("fastestLapTime","fastest_lap")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Join race and circuits df

# COMMAND ----------

races_cirtuits_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id)\
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(races_cirtuits_df,results_df.race_id==races_cirtuits_df.race_id)\
                          .join(drivers_df,results_df.driver_id==drivers_df.driver_id)\
                          .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df=race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name",
                                "driver_number", "driver_nationality", "team", "grid", "race_time", "points", "fastest_lap","position")\
                                 .withColumn("created_date",current_timestamp())

# COMMAND ----------

final_df=final_df.orderBy(final_df.points.desc())

# COMMAND ----------

display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------


