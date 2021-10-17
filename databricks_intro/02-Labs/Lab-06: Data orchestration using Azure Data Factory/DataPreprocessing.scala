// Databricks notebook source
//BLOB CONFIGURATION
val storageAccountName = "nytaxidata"
val containerName = "taxidata"
var fileName = "yellow_tripdata_2017-01.csv"
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)
val blobFilePath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/${fileName}"
val dataFromBlob= spark.read.option("header","true").csv(blobFilePath)
display(dataFromBlob)

// COMMAND ----------

// VIEW SAMPLE DATA FOR TRIPS
val tripsDF = dataFromBlob
display(tripsDF)

// COMMAND ----------

//Perform a select query with cast function on particular columns
import org.apache.spark.sql.types._
val tripsCastDF=tripsDF.select($"VendorID",
                               $"tpep_pickup_datetime".cast(TimestampType),
                               $"tpep_dropoff_datetime".cast(TimestampType),
                               $"passenger_count".cast(IntegerType),
                               $"trip_distance".cast(DoubleType),
                               $"RatecodeID",
                               $"store_and_fwd_flag",
                               $"PULocationID".cast(IntegerType),
                               $"DOLocationID".cast(IntegerType),
                               $"payment_type".cast(IntegerType),
                               $"fare_amount".cast(DoubleType),
                               $"extra".cast(DoubleType),
                               $"mta_tax".cast(DoubleType),
                               $"tip_amount".cast(DoubleType),
                               $"tolls_amount".cast(DoubleType),
                               $"improvement_surcharge".cast(DoubleType),
                               $"total_amount".cast(DoubleType))

// COMMAND ----------

import org.apache.spark.sql.functions._
//Filter the data to get rows without the outliers
val filteredtripsCastDF = tripsCastDF.filter(($"fare_amount"<=100) && ($"fare_amount">0))
//Calculate average fare for all valid fares
val mean=filteredtripsCastDF.agg(avg($"fare_amount")).first.getDouble(0)
//Insert average fare in place of outliers
val correcttripsDF=tripsCastDF.select($"*", when($"fare_amount">100, mean).otherwise($"fare_amount").alias("fare"))
                          .drop($"fare_amount").withColumnRenamed("fare","fare_amount")

// COMMAND ----------

//Total amount as a sum of fare amount, extra, MTQ, Tips, Tolls and Improvement surcharge
val correctTotalTripsDF=correcttripsDF.withColumn("total",$"fare_amount"+$"extra"+$"mta_tax"+$"tip_amount"+$"tolls_amount"+$"tip_amount"+$"improvement_surcharge").drop("total_amount").withColumnRenamed("total","total_amount")

// COMMAND ----------

//Filtering for records that have pickup and drop time as null
val nullRecordsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNull or $"tpep_dropoff_datetime".isNull)
val nullRecordsCount=nullRecordsDF.count

// COMMAND ----------

//Filtering out records where pickup and dropoff date is null
val filterTripsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNotNull or $"tpep_dropoff_datetime".isNotNull)

// COMMAND ----------

//Filtering out records where Passenger count is not zero
val filteredTripsDF=filterTripsDF.filter($"passenger_count"=!=0)

// COMMAND ----------

// Write final table into Databricks File System
filteredTripsDF.write.mode("overwrite").format("parquet").save("dbfs:/FileStore/mlpipilines/Processdata")
