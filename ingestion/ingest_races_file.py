# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1: Create a schema to infer

# COMMAND ----------

# Creating a schema object to infer for races data

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                               StructField("year",IntegerType(),True),
                               StructField("round",IntegerType(),True),
                               StructField("circuitId",IntegerType(),True),
                               StructField("name",StringType(),True),
                               StructField("date",DateType(),True),
                               StructField("time",StringType(),True),
                               StructField("url",StringType(),True)
                              ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read the file and apply the schema

# COMMAND ----------

# create the races dataframe 

races_df=spark.read.schema(races_schema).csv("/mnt/storagegen2databricks/raw/races.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create the new columns

# COMMAND ----------

# Creating some new columns

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit
races_new_df=races_df.withColumn("ingestion_date",current_timestamp())\
                              .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df=races_new_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Rename the columns

# COMMAND ----------

races_final_df=races_selected_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step6: Write the data to parquet

# COMMAND ----------

races_final_df.write.parquet("/mnt/storagegen2databricks/processed/races",mode="overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/storagegen2databricks/processed/races

# COMMAND ----------

df=spark.read.parquet("/mnt/storagegen2databricks/processed/races")
display(df)

# COMMAND ----------


