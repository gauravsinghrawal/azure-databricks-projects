# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC println()
# MAGIC println()
# MAGIC println("Note: You can ignore the message 'Warning: classes defined within packages cannot be redefined without a cluster restart.'")

# COMMAND ----------

# MAGIC %scala
# MAGIC package com.databricks.training
# MAGIC object ClassroomHelper {
# MAGIC   def test_connection(url : String) : Unit = {
# MAGIC     val conn = java.sql.DriverManager.getConnection(url)
# MAGIC     try {
# MAGIC       val ps = conn.prepareStatement("SELECT 1")
# MAGIC       val rs = ps.executeQuery()
# MAGIC     } finally {
# MAGIC       conn.close()
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   def sql_query(spark: org.apache.spark.sql.SparkSession, url : String, sql : String) : org.apache.spark.sql.DataFrame = {
# MAGIC     return spark.read.jdbc(url, s"($sql) query", new java.util.Properties)
# MAGIC   }
# MAGIC 
# MAGIC   def sql_update(url : String, sql : String, args : Any*) : Unit = {
# MAGIC     val conn = java.sql.DriverManager.getConnection(url)
# MAGIC     try {
# MAGIC       val ps = conn.prepareStatement(sql)
# MAGIC       args.zipWithIndex.foreach {case (arg, i) => {
# MAGIC         ps.setObject(i+1, arg)
# MAGIC       }}
# MAGIC       ps.executeUpdate()
# MAGIC     } finally {
# MAGIC       conn.close()
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   def sql_update(url : String, sql : String, args : java.util.ArrayList[Object]) : Unit = {
# MAGIC     import scala.collection.JavaConverters._
# MAGIC     return sql_update(url, sql, args.asScala:_*)
# MAGIC   }
# MAGIC 
# MAGIC   def sql(url : String, sql : String) : Unit = {
# MAGIC     val conn = java.sql.DriverManager.getConnection(url)
# MAGIC     val stmt = conn.createStatement()
# MAGIC     val cmds = sql.split(";")
# MAGIC     var count = 0;
# MAGIC     try {
# MAGIC       for (cmd <- cmds) {
# MAGIC         if (!cmd.trim().isEmpty()) {
# MAGIC           stmt.addBatch(cmd)
# MAGIC           count += 1
# MAGIC         }
# MAGIC       }
# MAGIC       stmt.executeBatch()
# MAGIC     } finally {
# MAGIC       conn.close()
# MAGIC     }
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.training.ClassroomHelper
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.spearfishtrainingstorage.blob.core.windows.net",
# MAGIC   "cJoNObBZt8C8xz2GwQmaHx25DmyRXAyd8TEp7/HDlT6jgt4+LeOjwYEhQ5SsCCrO0HRy6xlL8WZEM6xEwE9+9Q==")
# MAGIC val blobStoreBaseURL = "wasbs://training-container-clean@spearfishtrainingstorage.blob.core.windows.net/"
# MAGIC val username = com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.BaseTagDefinitions.TAG_USER);
# MAGIC val userhome = s"dbfs:/user/$username/"
# MAGIC spark.conf.set("spark.databricks.delta.preview.enabled", true)
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
# MAGIC displayHTML("") //Supress Output

# COMMAND ----------

# MAGIC %python
# MAGIC class ClassroomHelper(object):
# MAGIC   scalaHelper=spark._jvm.__getattr__("com.databricks.training.ClassroomHelper$").__getattr__("MODULE$")
# MAGIC   @classmethod
# MAGIC   def test_connection(cls, url):
# MAGIC     cls.scalaHelper.test_connection(url)
# MAGIC   @classmethod
# MAGIC   def sql_query(cls, spark, url, sql):
# MAGIC     return spark.read.jdbc(url, "({}) query".format(sql))
# MAGIC   @classmethod
# MAGIC   def sql_update(cls, url, sql, *args):
# MAGIC     cls.scalaHelper.sql_update(url, sql, args)
# MAGIC   @classmethod
# MAGIC   def sql(cls, url, sql):
# MAGIC     cls.scalaHelper.sql(url, sql)
# MAGIC 
# MAGIC blobStoreBaseURL = "wasbs://training-container-clean@spearfishtrainingstorage.blob.core.windows.net/"
# MAGIC username = spark.conf.get("com.databricks.training.username")
# MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
# MAGIC None #Suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Database-Setup successful.")
