# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Circuits File

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
v_file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read using spark dataframe reader

# COMMAND ----------

circuits_df=spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv",header=True,inferSchema=True)

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
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 - Add a new time column

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as a Parquet file

# COMMAND ----------

#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode="overwrite")
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


