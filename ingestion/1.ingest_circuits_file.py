# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Circuits File

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read using spark dataframe reader

# COMMAND ----------

circuits_df=spark.read.csv(f"{raw_folder_path}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Selecting the required columns

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 - Add a new time column

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as a Parquet file

# COMMAND ----------

circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode="overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/storagegen2databricks/processed/circuits

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
