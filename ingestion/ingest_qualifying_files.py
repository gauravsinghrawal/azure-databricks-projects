# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step1: Define Schema and read the multiple, multi-line JSON file.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

qualifying_schema=StructType(fields=[StructField("qualifyingId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("q1",StringType(),True),
                                 StructField("q2",StringType(),True),
                                 StructField("q3",StringType(),True)                                  
                                ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Read multiple multiline JSON files

# COMMAND ----------

qualifying_df=spark.read \
.schema(qualifying_schema) \
.option("multiLine",True) \
.json("/mnt/storagegen2databricks/raw/qualifying/qualifying_split*.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

qualifying_final_df=qualifying_df.withColumnRenamed("qualifyingId","qualifying_id") \
              .withColumnRenamed("raceId","race_id") \
              .withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("constructorId","constructor_id") \
              .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.parquet("/mnt/storagegen2databricks/processed/qualifying",mode="overwrite")

# COMMAND ----------


