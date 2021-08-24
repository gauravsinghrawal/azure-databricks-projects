# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Read the JSON file.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                              StructField("surname",StringType(),True)
                              ])
driver_schema=StructType(fields=[StructField("driverId",IntegerType(),True),
                                 StructField("driverRef",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("code",StringType(),True),
                                 StructField("name",name_schema,True),
                                 StructField("dob",DateType(),True),
                                 StructField("nationality",StringType(),True),
                                 StructField("url",StringType(),True)
                              ])


# COMMAND ----------

drivers_df=spark.read \
.schema(driver_schema) \
.json("/mnt/storagegen2databricks/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,lit,col

new_drivers_df=drivers_df.withColumnRenamed("driverId","driver_id") \
                          .withColumnRenamed("driverRef","driver_ref") \
                          .withColumn("ingestion_date",current_timestamp())\
                          .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Drop the unwanted columns

# COMMAND ----------

drivers_final_df=new_drivers_df.drop(new_drivers_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Write the data to parquet

# COMMAND ----------

drivers_final_df.write.parquet("/mnt/storagegen2databricks/processed/drivers",mode="overwrite")

# COMMAND ----------


