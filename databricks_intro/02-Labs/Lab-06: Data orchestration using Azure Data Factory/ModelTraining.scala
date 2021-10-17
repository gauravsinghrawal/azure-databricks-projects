// Databricks notebook source
//Read the data from dbfs in parquet format
val trainingData = spark.read.option("header","true").parquet("dbfs:/FileStore/mlpipilines/trainedData")

// COMMAND ----------

// DBTITLE 1,Linear Regression
//trained the model data
import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression()
// We may set parameters using setter methods.
            .setLabelCol("actuals")
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
// Print out the parameters, documentation, and any default values.
println(s"Linear Regression parameters:\n ${lr.explainParams()}\n")
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingData)

// COMMAND ----------

//save the trained model into mlpiplines folder
val modelPath = "/FileStore/mlpipilines/mlmodel/"
lrModel.write.overwrite.save(modelPath)
