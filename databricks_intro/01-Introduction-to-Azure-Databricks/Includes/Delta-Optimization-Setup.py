# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC from pyspark.sql.functions import expr, col, from_unixtime, to_date
# MAGIC 
# MAGIC dbutils.fs.rm(userhome + "/delta/iot-events/", True)
# MAGIC 
# MAGIC streamingEventPath = "/mnt/training/structured-streaming/events/"
# MAGIC 
# MAGIC (spark
# MAGIC   .read
# MAGIC   .option("inferSchema", "true")
# MAGIC   .json(streamingEventPath)
# MAGIC   .withColumn("date", to_date(from_unixtime(col("time").cast("Long"),"yyyy-MM-dd")))
# MAGIC   .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
# MAGIC   .repartition(200)
# MAGIC   .write
# MAGIC   .mode("overwrite")
# MAGIC   .format("delta")
# MAGIC   .partitionBy("date")
# MAGIC   .save(userhome + "/delta/iot-events/"))

# COMMAND ----------


