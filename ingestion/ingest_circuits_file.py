# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Circuits File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read using spark dataframe reader

# COMMAND ----------

circuits_df=spark.read.csv("/mnt/storagegen2databricks/raw/circuits.csv",header=True,inferSchema=True)

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

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 - Add a new time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as a Parquet file

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/storagegen2databricks/processed/circuits",mode="overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/storagegen2databricks/processed/circuits

# COMMAND ----------

df=spark.read.parquet("/mnt/storagegen2databricks/processed/circuits")
display(df)

# COMMAND ----------


