# Databricks notebook source
circuits_ingest_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

circuits_ingest_result

# COMMAND ----------

constructor_ingest_result = dbutils.notebook.run("2.ingest_constructors_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

constructor_ingest_result

# COMMAND ----------

drivers_ingest_result = dbutils.notebook.run("3.ingest_drivers_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

drivers_ingest_result

# COMMAND ----------

laptimes_ingest_result = dbutils.notebook.run("4.ingest_lap_times_files",0,{"data_source":"Ergast API"})

# COMMAND ----------

laptimes_ingest_result

# COMMAND ----------

pitstops_ingest_result = dbutils.notebook.run("5.ingest_pitstops_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

pitstops_ingest_result

# COMMAND ----------

qualifying_ingest_result = dbutils.notebook.run("6.ingest_qualifying_files",0,{"data_source":"Ergast API"})

# COMMAND ----------

qualifying_ingest_result

# COMMAND ----------

races_ingest_result = dbutils.notebook.run("7.ingest_races_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

races_ingest_result

# COMMAND ----------

results_ingest_result = dbutils.notebook.run("8.ingest_results_file",0,{"data_source":"Ergast API"})

# COMMAND ----------

results_ingest_result
