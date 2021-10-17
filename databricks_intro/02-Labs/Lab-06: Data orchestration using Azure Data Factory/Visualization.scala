// Databricks notebook source
import org.apache.spark.ml.regression.LinearRegressionModel
val lrModel = LinearRegressionModel.load("/FileStore/mlpipilines/mlmodel/")
val testData = spark.read.option("header","true").parquet("dbfs:/FileStore/mlpipilines/testData")

// COMMAND ----------

display(lrModel, testData, plotType="fittedVsResiduals")
