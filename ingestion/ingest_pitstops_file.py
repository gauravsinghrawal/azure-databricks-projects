# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step1: Define Schema and read the multi-line JSON file.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

pitstop_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("duration",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True)
                                ])

# COMMAND ----------

pitstops_df=spark.read \
.schema(pitstop_schema) \
.option("multiLine",True) \
.json("/mnt/storagegen2databricks/raw/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pitstop_final_df=pitstops_df.withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("raceId","race_id") \
              .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

pitstop_final_df.write.parquet("/mnt/storagegen2databricks/processed/pitstops",mode="overwrite")

# COMMAND ----------


