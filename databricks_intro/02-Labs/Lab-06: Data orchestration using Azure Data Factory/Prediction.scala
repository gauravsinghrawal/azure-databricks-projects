// Databricks notebook source
//Load the tain model and test data
import org.apache.spark.ml.regression.LinearRegressionModel
val lrModel = LinearRegressionModel.load("/FileStore/mlpipilines/mlmodel/")
val testData = spark.read.option("header","true").parquet("dbfs:/FileStore/mlpipilines/testData")

// COMMAND ----------

// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column.
val lrPredictions = lrModel.transform(testData)

// COMMAND ----------

//display the prediction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
println("\nPredictions : " )
	lrPredictions.select($"actuals".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"actuals")).distinct.show(15)
