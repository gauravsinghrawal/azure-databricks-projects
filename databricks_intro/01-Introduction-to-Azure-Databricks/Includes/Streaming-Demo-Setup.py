# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.types import StringType, DoubleType, LongType
# MAGIC 
# MAGIC dbutils.fs.rm(userhome + "/streaming-demo", True)
# MAGIC 
# MAGIC activityDataDF = (spark.read
# MAGIC                   .json("/mnt/training/definitive-guide/data/activity-data-with-geo.json/"))
# MAGIC 
# MAGIC tempJson = spark.createDataFrame(activityDataDF.toJSON(), StringType()).withColumnRenamed("value", "body")
# MAGIC 
# MAGIC tempJson.write.mode("overwrite").format("delta").save(userhome + "/streaming-demo")
# MAGIC 
# MAGIC activityStreamDF = (spark.readStream
# MAGIC   .format("delta")
# MAGIC   .option("maxFilesPerTrigger", 1)
# MAGIC   .load(userhome + "/streaming-demo")
# MAGIC )
