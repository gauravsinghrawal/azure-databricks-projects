# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

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
.json(f"{raw_folder_path}/qualifying/qualifying_split*.json")

# COMMAND ----------

qualifying_renamed_df=qualifying_df.withColumnRenamed("qualifyingId","qualifying_id") \
              .withColumnRenamed("raceId","race_id") \
              .withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("constructorId","constructor_id")

# COMMAND ----------

qualifying_final_df=add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df.write.parquet(f"{processed_folder_path}/qualifying",mode="overwrite")

# COMMAND ----------


