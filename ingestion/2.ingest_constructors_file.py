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
# MAGIC ### Step 1: Read the JSON file using databricks Reader method

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_df=constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

constructor_renamed_df=constructors_df.withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("constructorRef","constructor_ref") \
                                    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

constructor_final_df=add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Write the data to a parquet file

# COMMAND ----------

constructor_final_df.write.parquet(f"{processed_folder_path}/constructors",mode="overwrite")

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/constructors")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")