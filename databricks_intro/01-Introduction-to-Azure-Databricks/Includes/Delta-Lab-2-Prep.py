# Databricks notebook source
# MAGIC 
# MAGIC %run "./Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Delta-Lab-1-Prep"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

DeltaPath = userhome + "/delta/customer-data/"
CustomerCountsPath = userhome + "/delta/customer_counts/"
dbutils.fs.rm(CustomerCountsPath, True)

(spark.read
  .format("delta")
  .load(DeltaPath)
  .groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders")
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(CustomerCountsPath))

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_counts
""")

spark.sql("""
  CREATE TABLE customer_counts
  USING DELTA
  LOCATION '{}'
""".format(CustomerCountsPath))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

# COMMAND ----------

newDataPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"

spark.sql("DROP TABLE IF EXISTS new_customer_counts")
newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(newDataPath)
)

(newDataDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders")
  .write
  .saveAsTable("new_customer_counts"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO customer_counts
# MAGIC USING new_customer_counts
# MAGIC ON customer_counts.Country = new_customer_counts.Country
# MAGIC AND customer_counts.CustomerID = new_customer_counts.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET total_orders = customer_counts.total_orders + new_customer_counts.total_orders
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

(newDataDF.write
  .format("delta")
  .mode("append")
  .save(DeltaPath))

