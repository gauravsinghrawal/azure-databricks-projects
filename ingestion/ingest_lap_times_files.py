# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step1: Define Schema and read the multiple CSV files.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True)
                                ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Apply the schema and read files using wildcard for all the files

# COMMAND ----------

laptimes_df=spark.read \
.schema(lap_times_schema) \
.csv("/mnt/storagegen2databricks/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Rename/Add the columns and write to parquet files

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

laptimes_final_df=laptimes_df.withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("raceId","race_id") \
              .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

laptimes_final_df.write.parquet("/mnt/storagegen2databricks/processed/laptimes",mode="overwrite")

# COMMAND ----------


