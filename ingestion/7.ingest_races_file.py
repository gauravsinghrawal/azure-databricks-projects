# Databricks notebook source
dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
v_file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

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

races_df=spark.read.schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create the new columns

# COMMAND ----------

# Creating some new columns

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit,col

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
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step6: Write the data to parquet

# COMMAND ----------

# Storing the data in partitions of years 

#races_final_df.write.partitionBy("race_year").parquet(f"{processed_folder_path}/races",mode="overwrite")
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
