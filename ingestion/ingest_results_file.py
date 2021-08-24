# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Define the schema and apply to the file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType

results_schema=StructType(fields=[StructField("resultId",IntegerType(),True),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                   StructField("positionText",StringType(),True),
                                   StructField("positionOrder",IntegerType(),True),
                                   StructField("points",DoubleType(),True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                   StructField("fastestLapTime",StringType(),True),
                                   StructField("fastestLapSpeed",StringType(),True),
                                   StructField("statusId",IntegerType(),True)
                              ])

# COMMAND ----------

results_df=spark.read\
          .schema(results_schema) \
          .json("/mnt/storagegen2databricks/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Rename and Add Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,lit,col

new_results_df=results_df.withColumnRenamed("resultId","result_id") \
                          .withColumnRenamed("raceId","race_id") \
                          .withColumnRenamed("driverId","driver_id")\
                          .withColumnRenamed("constructorId","constructor_id")\
                          .withColumnRenamed("positionText","position_text")\
                          .withColumnRenamed("positionOrder","position_order")\
                          .withColumnRenamed("fastestLap","fastest_lap")\
                          .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                          .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                          .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

results_final_df=new_results_df.drop("statusId")

# COMMAND ----------

display(final_results_df)

# COMMAND ----------

results_final_df.write.partitionBy("race_id").parquet("/mnt/storagegen2databricks/processed/results",mode="overwrite")

# COMMAND ----------

display(spark.read.parquet("/mnt/storagegen2databricks/processed/results"))

# COMMAND ----------

