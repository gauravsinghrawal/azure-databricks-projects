// Databricks notebook source
// DBTITLE 1,Load Data
//Read the data from dbfs in parquet format
val tripdata = spark.read.option("header","true").parquet("FileStore/mlpipilines/Processdata/")

// COMMAND ----------

//view the display data
display(tripdata)

// COMMAND ----------

// DBTITLE 1,Preprocess the data
//Preprocess the data
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val data=tripdata.select($"*",$"tpep_pickup_datetime".cast(TimestampType).alias("pickup_time"),$"tpep_dropoff_datetime".cast(TimestampType).alias("dropoff_time"),$"fare_amount".cast(DoubleType).alias("fare")).drop("tpep_pickup_datetime","tpep_dropoff_datetime","","fare_amount")

val selectData=data.select($"pickup_time",$"pickup_time".cast(LongType).alias("pickup"),$"dropoff_time",$"dropoff_time".cast(LongType)alias("dropoff"),$"trip_distance".cast(DoubleType),$"PULocationID".cast(IntegerType),$"DOLocationID".cast(IntegerType),$"RatecodeID".cast(IntegerType),$"store_and_fwd_flag",$"payment_type".cast(IntegerType),$"fare",$"extra".cast(DoubleType))
.withColumn("duration",unix_timestamp($"dropoff_time")-unix_timestamp($"pickup_time"))
.withColumn("pickup_hour",hour($"pickup_time"))
.withColumn("pickup_day",dayofweek($"pickup_time"))
.withColumn("pickup_day_month",dayofmonth($"pickup_time"))
.withColumn("pickup_minute",minute($"pickup_time"))
.withColumn("pickup_weekday",weekofyear($"pickup_time"))
.withColumn("pickup_month",month($"pickup_time"))
.filter($"fare">0 && $"duration">0 && $"fare"<30000)
.drop("pickup","dropoff_time","pickup_time")

// COMMAND ----------

// DBTITLE 1,Data Preparation and Feature Transformation
//The dataset has categorical data in the PULocationID, DOLocationID, store_and_fwd_flag and RatecodeID columns. We will use a string indexer to convert these to numeric labels

import org.apache.spark.ml.feature.StringIndexer

val indexerPickup = new StringIndexer()
                   .setInputCol("PULocationID")
                   .setOutputCol("PUid")
                   .fit(selectData)
val indexerDropoff= new StringIndexer()
                    .setInputCol("DOLocationID")
                    .setOutputCol("DOid")
                    .fit(selectData)
val indexerRatecode= new StringIndexer()
                    .setInputCol("RatecodeID")
                    .setOutputCol("Ratecode")
                    .fit(selectData)
val indexerStoreFlag=new StringIndexer()
                    .setInputCol("store_and_fwd_flag")
                    .setOutputCol("storeFlag")
                    .fit(selectData)
val indexedData=indexerPickup.transform(
                   indexerDropoff.transform(
                     indexerStoreFlag.transform(
                       indexerRatecode.transform(selectData))))

// COMMAND ----------

display(indexedData)

// COMMAND ----------

// DBTITLE 1,Encoding
//encode categorical features using StringIndexer first
import org.apache.spark.ml.feature.OneHotEncoderEstimator
val encoder = new OneHotEncoderEstimator()
  .setInputCols(Array("PUid", "DOid","Ratecode","storeFlag"))
  .setOutputCols(Array("pickup_loc", "drop_loc","RatecodeFlag","storeFwdFlag"))
  .fit(indexedData)
val encodedData = encoder.transform(indexedData)

// COMMAND ----------

encodedData.show

// COMMAND ----------

// DBTITLE 1,Assembling into Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val featureCols=Array("pickup_loc","trip_distance","pickup_hour","pickup_day","pickup_day_month","pickup_minute","pickup_weekday","pickup_month","RatecodeID")
val assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val output = assembler.transform(encodedData)
val assembledData=output.select("duration","features")

// COMMAND ----------

display(assembledData)

// COMMAND ----------

// DBTITLE 1,Normalizing the vectors
import org.apache.spark.ml.feature.Normalizer

val normalizedData = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .transform(assembledData)

// COMMAND ----------

display(normalizedData.select("duration","normFeatures"))

// COMMAND ----------

// DBTITLE 1,Indexing Label
import org.apache.spark.ml.feature.StringIndexer
val labelIndexer = new StringIndexer().setInputCol("duration").setOutputCol("actuals")
val transformedData = labelIndexer.fit(normalizedData).transform(normalizedData)

// COMMAND ----------

display(transformedData.select("actuals","normFeatures"))

// COMMAND ----------

// DBTITLE 1,Split data into training and test datasets
val Array(trainingData, testData) = transformedData.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

display(trainingData)

// COMMAND ----------

// Write final table into Databricks File System
trainingData.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/mlpipilines/trainedData")
testData.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/mlpipilines/testData")
