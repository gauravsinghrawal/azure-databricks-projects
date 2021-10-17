# Databricks notebook source
# MAGIC 
# MAGIC %run "./Classroom-Setup"

# COMMAND ----------

inputPath = "/mnt/training/online_retail/data-001/data.csv"
DataPath = userhome + "/delta/customer-data/"

#remove directory if it exists
dbutils.fs.rm(DataPath, True)

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

(spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(DataPath))

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql("""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION '{}'
""".format(DataPath))

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

(spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)
)

# COMMAND ----------

spark.read.format("json").load("/mnt/training/enb/commonfiles/upsert-data.json").createOrReplaceTempView("upsert_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_data_delta
# MAGIC USING upsert_data
# MAGIC ON customer_data_delta.InvoiceNo = upsert_data.InvoiceNo
# MAGIC   AND customer_data_delta.StockCode = upsert_data.StockCode
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *
