# Databricks notebook source
dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

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
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

pitstop_renamed_df=pitstops_df.withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("raceId","race_id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

pitstop_final_df=add_ingestion_date(pitstop_renamed_df)

# COMMAND ----------

#pitstop_final_df.write.parquet(f"{processed_folder_path}/pitstops",mode="overwrite")
pitstop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")
