# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC {val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC 
# MAGIC //*******************************************
# MAGIC // GET USERNAME AND USERHOME
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get the user's name
# MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC // Set the user's name and home directory
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)}
# MAGIC 
# MAGIC print("Success!\n")
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.username")
# MAGIC val userhome = spark.conf.get("com.databricks.training.userhome")

# COMMAND ----------

# MAGIC %python
# MAGIC username = spark.conf.get("com.databricks.training.username")
# MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
# MAGIC 
# MAGIC None # suppress output
