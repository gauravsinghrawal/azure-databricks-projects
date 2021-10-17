# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step1: Define Schema and read the multiple CSV files.

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.csv(f"{raw_folder_path}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Rename/Add the columns and write to parquet files

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

laptimes_renamed_df=laptimes_df.withColumnRenamed("driverId","driver_id") \
              .withColumnRenamed("raceId","race_id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

laptimes_final_df=add_ingestion_date(laptimes_renamed_df)

# COMMAND ----------

#laptimes_final_df.write.parquet(f"{processed_folder_path}/laptimes",mode="overwrite")
laptimes_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success")
