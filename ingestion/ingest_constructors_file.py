# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Read the JSON file using databricks Reader method

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read\
.schema(constructors_schema)\
.json("/mnt/storagegen2databricks/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_df=constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df=constructors_df.withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("constructorRef","constructor_ref")\
                                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Write the data to a parquet file

# COMMAND ----------

constructor_final_df.write.parquet("/mnt/storagegen2databricks/processed/constructors",mode="overwrite")

# COMMAND ----------

df=spark.read.parquet("/mnt/storagegen2databricks/processed/constructors")
display(df)

# COMMAND ----------


