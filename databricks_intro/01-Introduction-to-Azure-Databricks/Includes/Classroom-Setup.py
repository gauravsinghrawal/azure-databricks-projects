# Databricks notebook source
# MAGIC 
# MAGIC %run "./Course-Name"

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %run "./Create-User-DB"

# COMMAND ----------

#*******************************************
# ILT Specific functions
#*******************************************

# Utility method to count & print the number of records in each partition.
# def printRecordsPerPartition(df):
#   print("Per-Partition Counts:")
#   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
#   results = (df.rdd                   # Convert to an RDD
#     .mapPartitions(countInPartition)  # For each partition, count
#     .collect()                        # Return the counts to the driver
#   )
#   # Print out the results.
#   for result in results: print("* " + str(result))

# None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC displayHTML("All done!")
