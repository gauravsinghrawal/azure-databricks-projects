// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # About this Tutorial #
// MAGIC 
// MAGIC ##### Goal: The goal of this tutorial us to help you understand the capabilities and features of Azure Spark MLlib for machine learning (ML).  
// MAGIC 
// MAGIC Azure Databricks Spark provides a general machine learning library -- MLlib -- that is designed for simplicity, scalability, and easy integration with other tools. With the scalability, language compatibility, and speed of Spark, data scientists can solve and iterate through their data problems faster.
// MAGIC 
// MAGIC MLlib is Spark’s machine learning (ML) library. Its goal is to make practical machine learning scalable and easy. At a high level, it provides tools such as:
// MAGIC 
// MAGIC - ML Algorithms: common learning algorithms such as classification, regression, clustering, and collaborative filtering
// MAGIC - Featurization: feature extraction, transformation, dimensionality reduction, and selection
// MAGIC - Pipelines: tools for constructing, evaluating, and tuning ML Pipelines
// MAGIC - Persistence: saving and load algorithms, models, and Pipelines
// MAGIC - Utilities: linear algebra, statistics, data handling, etc.
// MAGIC 
// MAGIC The advantages of MLlib’s design include:
// MAGIC 
// MAGIC - **Simplicity:** Simple APIs familiar to data scientists coming from tools like R and Python. Novices are able to run algorithms out of the box while experts can easily tune the system by adjusting important knobs and switches (parameters).
// MAGIC - **Scalability:** Ability to run the same ML code on your laptop and on a big cluster seamlessly.
// MAGIC - **Streamlined end-to-end:** Developing machine learning models is a multistep journey from data ingest through trial and error to production. Building MLlib on top of Spark makes it possible to tackle these distinct needs with a single tool instead of many disjointed ones. The advantages are lower learning curves, less complex development and production environments, and ultimately shorter times to deliver high-performing models.
// MAGIC - **Compatibility:** Data scientists often have workflows built up in common datascience tools such as R, Python pandas and scikit-learn. Spark DataFrames and MLlib provide tooling that makes it easier to integrate these existing workflows with Spark. For example, SparkR allows users to call MLlib algorithms using familiar R syntax, and Databricks is writing Spark packages in Python to allow users to distribute parts of scikit-learn workflows.
// MAGIC 
// MAGIC ##### Data: We will use the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) from the previous tutorial. 
// MAGIC 
// MAGIC ##### Tasks: In this tutorial you will be performing the following tasks: #####
// MAGIC 
// MAGIC The end-to-end process for building and refining a machine learning model can be quite involved. The major steps include the following:
// MAGIC - Acquire the data from a data source.
// MAGIC - Preprocess the data
// MAGIC - Explore the data to see if there is any correlation between the data features 
// MAGIC - Transform the data (also known as Feature Transformation) so that it is optimized for consumption by Spark MLlib for model training. Note that transformation can itself involve multiple steps, with each step transforming different parts of the data in different ways. Here, the following transformations will be performed.
// MAGIC    - Indexing
// MAGIC    - Encoding
// MAGIC    - Assembling into vectors
// MAGIC    - Normalizing the vectors
// MAGIC - Select an algorithm to train the model. Here we will use ** Linear Regression **.
// MAGIC - Build and Train the model. 
// MAGIC   - Here The model will predict the ** the duration of a trip** based on pickup time, pickup location and dropoff location.
// MAGIC - Evaluate the efficiency (also known as performance) of the model. 
// MAGIC   - Here we will use ** Root Mean Squared Error ** evaluation function
// MAGIC - Refine the model for better performance. 
// MAGIC   - Model refinement can be done in multiple ways. You can change the parameters that define the model or change the algorithm altogether
// MAGIC   - Here we will try to build a better model using the ** Random Forest ** algorithm.
// MAGIC - Use the ** K-means ** algorithm to see if there are any natural ** clusters ** in ** duration and trip distance ** 
// MAGIC - Visualize the clusters
// MAGIC 
// MAGIC Note: In the real-world, building high-performance models is often an iterative, trial-and-error process. Often times all of the above steps may have to be repeated multiple times before you arrive at the optimal model<br/><br/>  

// COMMAND ----------

// MAGIC %md
// MAGIC ###Prerequisite: 
// MAGIC The tutorial requires that you have access to an ** Azure Blob Storage account ** that contains the NY Taxi Data.  
// MAGIC Configure the access to the account in cmd 3 below.

// COMMAND ----------

//Configure access to the storage account here.
val storageAccountName = "nytaxidata"
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="

val containerName = "taxidata"
val yellow_trip_data = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-*.csv"
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

// MAGIC %md ##Dataset Review##
// MAGIC 
// MAGIC This dataset contains detailed trip-level data on New York City Taxi trips. It was collected from both drivers inputs as well as the GPS coordinates of individual cabs. 
// MAGIC 
// MAGIC 
// MAGIC **The data is in a CSV format and has the following fields:**
// MAGIC 
// MAGIC 
// MAGIC * tripID: a unique identifier for each trip
// MAGIC * VendorID: a code indicating the provider associated with the trip record
// MAGIC * tpep_pickup_datetime: date and time when the meter was engaged
// MAGIC * tpep_dropoff_datetime: date and time when the meter was disengaged
// MAGIC * passenger_count: the number of passengers in the vehicle (driver entered value)
// MAGIC * trip_distance: The elapsed trip distance in miles reported by the taximeter
// MAGIC * RatecodeID: The final rate code in effect at the end of the trip -1= Standard rate -2=JFK -3=Newark -4=Nassau or Westchester -5=Negotiated fare -6=Group ride
// MAGIC * store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip
// MAGIC * PULocationID: TLC Taxi Zone in which the taximeter was engaged
// MAGIC * DOLocationID: TLC Taxi Zone in which the taximeter was disengaged
// MAGIC * payment_type: A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip
// MAGIC * fare_amount: The time-and-distance fare calculated by the meter.
// MAGIC * extra: Miscellaneous extras and surcharges
// MAGIC * mta_tax: $0.50 MTA tax that is automatically triggered based on the metered rate in use
// MAGIC * tip_amount: Tip amount –This field is automatically populated for credit card tips. Cash tips are not included
// MAGIC * tolls_amount:Total amount of all tolls paid in trip
// MAGIC * improvement_surcharge: $0.30 improvement surcharge assessed trips at the flag drop.
// MAGIC * total_amount: The total amount charged to passengers. Does not include cash tips.

// COMMAND ----------

// MAGIC %md ##Load Data

// COMMAND ----------

// MAGIC %md We will read the cab data from Blob Storage using the CSV data source of Spark

// COMMAND ----------

//val containerName = "taxidata"
//val wasbsPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-*.csv"
val tripdata = spark.read.option("header", true).csv(yellow_trip_data)

// COMMAND ----------

// MAGIC %md # Preprocess the data
// MAGIC 
// MAGIC Select relevent features according to use case and typecast numeric strings to Integer, Double and Timestamp so that they can be used for further processing.
// MAGIC We now have pickup and dropoff datetime in Timestamp format. We will use SparkSQL's datetime functions to extract minutes ,hour, day of week, day of month and weekday from the pickup datetime to use as features for our machine learning model.
// MAGIC We will also calculate the difference between the pickup and the dropoff time to get the time duration taken to complete each trip. This value will be saved in a column called `duration`. Further in this tutorial,  we will use the `duration` column as our label, i.e we will perform predictions for this column using other columns as our features. 
// MAGIC 
// MAGIC Since we are working on the NYC Cab Dataset, an interesting scenario to perform in SparkML will be to predict the time being taken to complete trips. We shall preprocess our data keeping according to this use case.

// COMMAND ----------

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
.filter($"fare">0 || $"duration">0 || $"fare"<5000)
.drop("pickup","dropoff_time","pickup_time")

// COMMAND ----------

// MAGIC %md ## Finding Correlation between different columns in our data ##
// MAGIC Correlation is used to test relationships between quantitative variables or categorical variables. In other words, it’s a measure of how things are related. Correlations are useful because if you can find out what relationship variables have, you can make predictions about future behavior.
// MAGIC 
// MAGIC Spark ML provides us with library functions to perform correlation and other basic statistic operations.

// COMMAND ----------

// MAGIC %md Correlation coefficients range between -1 and +1. The closer the value is to +1, the higher the positive correlation is, i.e the values are more related to each other. The closer the correlation coefficient is to -1, the more the negative correlation is. It can be mentioned here that having a negative correlation does not mean the values are less related, just that one variable increases as the other decreases.
// MAGIC 
// MAGIC For example, a correlation of -0.8 is better than a correlation of 0.4 and the values are more related in the first case than in the second.

// COMMAND ----------

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

val ls= new ListBuffer[(Double, String)]()
println("\nCorrelation Analysis :")
   	for ( field <-  selectData.schema) {
		if ( ! field.dataType.equals(StringType)) {
          var x= selectData.stat.corr("duration", field.name)
          var tuple : (Double, String) = (x,field.name)
          ls+=tuple
		}
	}
//Let us sort the correlation values in descending order to see the fields in the order of their relation with the duration field
// Sorting our correlation values in descending order to see the most related fields first
val lsMap= ls.toMap
val sortedMap= ListMap(lsMap.toSeq.sortWith(_._1 > _._1):_*)
sortedMap.collect{
  case (value, field_name) => println("Correlation between duration and " + field_name + " = " + value)
}

// COMMAND ----------

// MAGIC %md From our correlation analysis, we can observe that the `duration` column has the highest correlation with `trip_distance`, followed by `RatecodeID`, `extra` and `fare` column, which makes sense as the time taken for a trip to complete is directly related to the distance travelled.

// COMMAND ----------

// MAGIC %md # Data Preparation and Feature Transformation#

// COMMAND ----------

// MAGIC %md Since we are going to try algorithms like Linear Regression, we will have to convert the categorical variables in the dataset into numeric variables. There are 2 ways we can do this.
// MAGIC 
// MAGIC Category Indexing
// MAGIC 
// MAGIC This is basically assigning a numeric value to each category from {0, 1, 2, ...numCategories-1}. This introduces an implicit ordering among your categories, and is more suitable for ordinal variables (eg: Poor: 0, Average: 1, Good: 2)
// MAGIC 
// MAGIC One-Hot Encoding
// MAGIC 
// MAGIC This converts categories into binary vectors with at most one nonzero value (eg: (Blue: [1, 0]), (Green: [0, 1]), (Red: [0, 0]))

// COMMAND ----------

// MAGIC %md ## Indexing
// MAGIC 
// MAGIC StringIndexer encodes a string column of labels to a column of label indices. The indices are in [0, numLabels), ordered by label frequencies, so the most frequent label gets index 0. The unseen labels will be put at index numLabels if user chooses to keep them. If the input column is numeric, we cast it to string and index the string values. When downstream pipeline components such as Estimator or Transformer make use of this string-indexed label, you must set the input column of the component to this string-indexed column name. In many cases, you can set the input column with setInputCol.
// MAGIC 
// MAGIC The dataset has categorical data in the PULocationID, DOLocationID, store_and_fwd_flag and RatecodeID columns. We will use a string indexer to convert these to numeric labels

// COMMAND ----------

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

// MAGIC %md This is how these columns were before applying String Indexer

// COMMAND ----------

display(selectData.select("PULocationID","DOLocationID","RatecodeID","store_and_fwd_flag"))

// COMMAND ----------

// MAGIC %md Let us see display columns that were transformed in `indexedData`. We can see that the categorical string values have been converted to numeric indexes.

// COMMAND ----------

display(indexedData.select("PUid","DOid","Ratecode","storeFlag"))

// COMMAND ----------

// MAGIC %md ##Encoding
// MAGIC 
// MAGIC One-hot encoding maps a categorical feature, represented as a label index, to a binary vector with at most a single one-value indicating the presence of a specific feature value from among the set of all feature values. This encoding allows algorithms which expect continuous features, such as Logistic Regression, to use categorical features. For string type input data, it is common to encode categorical features using StringIndexer first.

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoderEstimator

val encoder = new OneHotEncoderEstimator()
  .setInputCols(Array("PUid", "DOid","Ratecode","storeFlag"))
  .setOutputCols(Array("pickup_loc", "drop_loc","RatecodeFlag","storeFwdFlag"))
  .fit(indexedData)

val encodedData = encoder.transform(indexedData)

// COMMAND ----------

// MAGIC %md Let us display our transformed columns in `encodedData` to see how applying the one-hot encoder has affected our data.

// COMMAND ----------

display(encodedData.select("pickup_loc","drop_loc","RatecodeFlag","storeFwdFlag"))

// COMMAND ----------

// MAGIC %md ##Assembling into Vectors
// MAGIC 
// MAGIC VectorAssembler is a transformer that combines a given list of columns into a single vector column. It is useful for combining raw features and features generated by different feature transformers into a single feature vector, in order to train ML models like logistic regression and decision trees. VectorAssembler accepts the following input column types: all numeric types, boolean type, and vector type. In each row, the values of the input columns will be concatenated into a vector in the specified order.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val featureCols=Array("pickup_loc","trip_distance","pickup_hour","pickup_day","pickup_day_month","pickup_minute","pickup_weekday","pickup_month","RatecodeID")
val assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val output = assembler.transform(encodedData)
val assembledData=output.select("duration","features")

// COMMAND ----------

// MAGIC %md The vector assembler combines all our features into a single column of type Vector. Our dataframe `assembledData` will have our label(i.e. the column on which predictions have to be made) that is `duration` and the all the features in vector form called `features`

// COMMAND ----------

display(assembledData)

// COMMAND ----------

// MAGIC %md ##Normalizing the vectors
// MAGIC 
// MAGIC Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which specifies the p-norm used for normalization. (p=2 by default.) This normalization can help standardize your input data and improve the behavior of learning algorithms.

// COMMAND ----------

import org.apache.spark.ml.feature.Normalizer

val normalizedData = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .transform(assembledData)

// COMMAND ----------

display(normalizedData.select("duration","normFeatures"))

// COMMAND ----------

// MAGIC %md ## Indexing Label ##

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
val labelIndexer = new StringIndexer().setInputCol("duration").setOutputCol("actuals")
val transformedData = labelIndexer.fit(normalizedData).transform(normalizedData)

// COMMAND ----------

// MAGIC %md Let us view our final dataframe with the duration in the column `actuals` and transformed, normalized features as `normFeatures`

// COMMAND ----------

display(transformedData.select("actuals","normFeatures"))

// COMMAND ----------

// MAGIC %md ###Split data into training and test datasets
// MAGIC 
// MAGIC You're splitting the data randomly to generate two sets: one to use during training of the ML algorithm (training set), and the second to check whether the training is working (test set). This is widely done and a very good idea, as it catches overfitting which otherwise can make it seem like you have a great ML solution when it's actually effectively just memorized the answer for each data point and can't interpolate or generalize.

// COMMAND ----------

val Array(trainingData, testData) = transformedData.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

// MAGIC %md Since training machine learning models is an iterative process, it is a good idea to cache our final transformed data in-memory so that the Spark engine does not have to fetch it again and again from the source. This will greatly reduce the time taken for training the model and improve performance.
// MAGIC 
// MAGIC We will cache our training dataset and call a count action to trigger the caching.

// COMMAND ----------

trainingData.cache
trainingData.count

// COMMAND ----------

// MAGIC %md #SUPERVISED LEARNING#
// MAGIC 
// MAGIC The majority of practical machine learning uses supervised learning.
// MAGIC 
// MAGIC Supervised learning is where you have input variables (x) and an output variable (Y) and you use an algorithm to learn the mapping function from the input to the output.
// MAGIC 
// MAGIC Y = f(X)
// MAGIC 
// MAGIC The goal is to approximate the mapping function so well that when you have new input data (x) that you can predict the output variables (Y) for that data.
// MAGIC 
// MAGIC It is called supervised learning because the process of an algorithm learning from the training dataset can be thought of as a teacher supervising the learning process. We know the correct answers, the algorithm iteratively makes predictions on the training data and is corrected by the teacher. Learning stops when the algorithm achieves an acceptable level of performance.
// MAGIC 
// MAGIC Supervised learning problems can be further grouped into regression and classification problems.
// MAGIC 
// MAGIC - Classification: A classification problem is when the output variable is a category, such as “red” or “blue” or “disease” and “no disease”.
// MAGIC - Regression: A regression problem is when the output variable is a real value, such as “dollars” or “weight”.

// COMMAND ----------

// MAGIC %md ## Linear Regression ##
// MAGIC Linear regression belongs to the family of regression algorithms. The goal of regression is to find relationships and dependencies between variables. It is modeling the relationship between a continuous scalar dependent variable y (also label or target in machine learning terminology) and one or more (a D-dimensional vector) explanatory variables (also independent variables, input variables, features, observed data, observations, attributes, dimensions, data point, ..) denoted X using a linear function. 
// MAGIC In regression analysis the goal is to predict a continuous target variable, whereas another area called classfication is predicting a label from a finite set. The model for a multiple regression which involves linear combination of input variables takes the form y = ß0 + ß1x1 + ß2x2 + ß3x3 + ..... + e . Linear regression belongs to a category of supervised learning algorithms. It means we train the model on a set of labeled data (training data) and then use the model to predict labels on unlabeled data (test data).
// MAGIC 
// MAGIC  The model (red line) is calculated using training data (blue points) where each point has a known label (y axis) to fit the points as accurately as possible by minimizing the value of a chosen loss function. We can then use the model to predict unknown labels (we only know x value and want to predict y value).
// MAGIC  
// MAGIC ![Linear Regression](https://nytaxidata.blob.core.windows.net/notebookimages/438px-Linear_regression.svg.png)

// COMMAND ----------

// MAGIC %md In this example we will use Linear Regression to predict the time duration taken for trips. The features we will use to train our model will include factors on which a trip duration is dependent; pickup Time, pickup location and dropoff location. We have already preprocessed and assembled these features into vectors in previous steps.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression()
// We may set parameters using setter methods.
            .setLabelCol("actuals")
            .setMaxIter(100)
// Print out the parameters, documentation, and any default values.
println(s"Linear Regression parameters:\n ${lr.explainParams()}\n")
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingData)
// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column.
val lrPredictions = lrModel.transform(testData)

// COMMAND ----------

// MAGIC %md Let us display a sample of 15 records of our predictions and the corresponding duration in Test data. We will order them by the difference between the actual and predicted values to see the predictions closest to the actual duration.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
println("\nPredictions : " )
	lrPredictions.select($"actuals".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"actuals")).distinct.show(15)

// COMMAND ----------

// MAGIC %md ##Residual Analysis
// MAGIC 
// MAGIC Because a linear regression model is not always appropriate for the data, you should assess the appropriateness of the model by defining residuals and examining residual plots.
// MAGIC 
// MAGIC The difference between the observed value of the dependent variable (y) and the predicted value (ŷ) is called the residual (e). Each data point has one residual.
// MAGIC 
// MAGIC #####Residual = Observed value - Predicted value ##### 
// MAGIC 
// MAGIC A residual plot is a graph that shows the residuals on the vertical axis and the independent variable on the horizontal axis. If the points in a residual plot are randomly dispersed around the horizontal axis, a linear regression model is appropriate for the data; otherwise, a non-linear model is more appropriate.
// MAGIC 
// MAGIC You can checkout [this](https://en.wikipedia.org/wiki/Errors_and_residuals) Wikipedia link to get an in-depth idea of residuals

// COMMAND ----------

// MAGIC %md The Fitted vs Residuals plot is available for Linear Regression and Logistic Regression models. The Databricks’ Fitted vs Residuals plot is analogous to [R’s “Residuals vs Fitted” plots for linear models](http://stat.ethz.ch/R-manual/R-patched/library/stats/html/plot.lm.html).
// MAGIC 
// MAGIC Linear Regression computes a prediction as a weighted sum of the input variables. Using Databrick's inbuilt visualization capabilities, the Fitted vs Residuals plot can be used to assess a linear regression model’s goodness of fit.

// COMMAND ----------

display(lrModel, testData, plotType="fittedVsResiduals")

// COMMAND ----------

// MAGIC %md In this scatter plot, the x-axis is the predicted values for the `duration` column made by our linear regression model, whereas the the y-axis shows the residual value,i.e. actual - predicted value for the predictions made by our linear regression model. In this graph, we can observe that the points are not dispersed randomly around the x-axis. We can infer from this that the linear model is not very appropriate for this data.

// COMMAND ----------

// MAGIC %md ## Evaluation ##
// MAGIC spark.mllib comes with a number of machine learning algorithms that can be used to learn from and make predictions on data. When these algorithms are applied to build machine learning models, there is a need to evaluate the performance of the model on some criteria, which depends on the application and its requirements. spark.mllib also provides a suite of metrics for the purpose of evaluating the performance of machine learning models. Here we will use the `RegressionEvaluator` to evaluate our Linear Regression Model. RegressionEvaluator has many metrics for model evaluation including:
// MAGIC 
// MAGIC  - ##### Root Mean Squared Error (RMSE) #####
// MAGIC  - ##### Mean Squared Error (MSE) #####
// MAGIC  - ##### Mean absolute Error #####
// MAGIC  - ##### Unadjusted Coefficient of Determination #####

// COMMAND ----------

//Evaluate the results. Calculate accuracy and coefficient of determination R2.
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("actuals")
//The metric we select for evaluation is the coefficient of determination
                .setMetricName("r2")
//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation.
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))


//Evaluate the results. Calculate Root Mean Square Error
val evaluator_rmse = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("actuals")
//The metric we select for evaluation is RMSE
                .setMetricName("rmse")
//As the name implies, isLargerBetter returns if a larger value is better for evaluation.
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))

// COMMAND ----------

// MAGIC %md Using Linear Regression we get a Root Mean Square Error of 505 seconds and  coefficient of determination of 0.48. For R2,`isLargerBetter` returns  `true` meaning a a better model will have a larger coefficient of determination. For RMSE, the `isLargerBetter` returns `false` meaning a smaller value of RMSE is more desirable.
// MAGIC 
// MAGIC Spark ML provides us with multiple algorithms for classification that we can use train our model. This gives us flexibility to try multiple algorithms to see which gives us better results.
// MAGIC 
// MAGIC Let us now perform the same prediction using Random Forest Regression algorithm.

// COMMAND ----------

// MAGIC %md ## Random Forest Regression Model ##
// MAGIC 
// MAGIC %md Random forests are ensembles of decision trees. Random forests combine many decision trees in order to reduce the risk of overfitting. The spark.ml implementation supports random forests for binary and multiclass classification and for regression, using both continuous and categorical features. Random Forest uses Decision Tree as the base model. Random Forests train each tree independently, using a random sample of the data. This randomness helps to make the model more robust than a single decision tree, and less likely to overfit on the training data.
// MAGIC 
// MAGIC For more information on the algorithm itself, please see the [spark.mllib documentation](https://spark.apache.org/docs/latest/mllib-ensembles.html#random-forests) on random forests.
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/RandomForest.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

//Create the model
val rf = new RandomForestRegressor()
                    .setLabelCol("actuals")
                    .setFeaturesCol("features")
//Number of trees in the forest
                    .setNumTrees(30)
//Maximum depth of each tree in the forest
                    .setMaxDepth(3)

// Note that you can create a better performing model by increasing the numTrees and the depth of each tree. For example you can set this to 100 and 15 respectively. 
// However, the model training will increase substantially. For values of 100 and 15, the training time could be as long as 1 hour.

val rfModel = rf.fit(trainingData)
	
//Predict on the test data
val rfPredictions = rfModel.transform(testData)

// COMMAND ----------

// MAGIC %md Let us view our a sample of 15 records of the predictions made by Random Forest Model with the corresponding actual duration ordered by their difference. We can compare these results with the previous ones computed by Linear Regression.

// COMMAND ----------

println("\nPredictions :")
rfPredictions.select($"actuals".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"actuals"-$"prediction")).distinct.show(15)

// COMMAND ----------

// MAGIC %md We can observe that our predictions are a lot closer to the actual values in this case than when we were using linear regression

// COMMAND ----------

// MAGIC %md Let us now evaluate our Random Forest Model to compare it with the previous model we trained using Linear Regression. We will calculate the same metric `R2` that is coefficient of determination and `RMSE` that is Root Mean Square Error for our Random Forest Model.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator()
               .setPredictionCol("prediction")
               .setLabelCol("actuals")
               .setMetricName("r2")
println("Coefficient Of determination = " + evaluator_r2.evaluate(rfPredictions))

val evaluator_rmse = new RegressionEvaluator()
               .setPredictionCol("prediction")
               .setLabelCol("actuals")
               .setMetricName("rmse")
println("RMSE = " + evaluator_rmse.evaluate(rfPredictions))

// COMMAND ----------

// MAGIC %md Our model trained using Random Forest Algorithm gives us a Root Mean Square Error of 425 seconds and coefficient of determination of 0.63. These evaluation parameters are better than what we got using Linear Regression. So, using Random Forest Algorithm for training has given us a better model to predict the time duration taken for each taxi to complete its trip.

// COMMAND ----------

// MAGIC %md ###MACHINE LEARNING MODEL PERSISTANCE###
// MAGIC Since Apache Spark 2.0, we also have model persistence, i.e the ability to save and load models. Spark’s Machine Learning library MLlib include near-complete support for ML persistence in the DataFrame-based API. Key features of ML persistence include:
// MAGIC 
// MAGIC - Support for all language APIs in Spark: Scala, Java, Python & R
// MAGIC - Support for nearly all ML algorithms in the DataFrame-based API
// MAGIC - Support for single models and full Pipelines, both unfitted (a “recipe”) and fitted (a result)
// MAGIC - Distributed storage using an exchangeable format
// MAGIC 
// MAGIC We can save a single model that can be shared between workspaces, notebooks and languages. To demonstrate this, we will save our trained Random Forest Algorithm to Azure Blob Storage so we can access it in the future without having to train the model again.

// COMMAND ----------

// val storageAccountName = "taxinydata"
// val containerName = "models"
//val modelPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/ml/"
val modelPath = "/FileStore/ml/models/"
// // We will use the save method of Dataframe API to persist our model.
rfModel.write.overwrite().save(modelPath)

// COMMAND ----------

// MAGIC %md # UNSUPERVISED LEARNING
// MAGIC 
// MAGIC Unsupervised learning is where you only have input data (X) and no corresponding output variables.
// MAGIC 
// MAGIC The goal for unsupervised learning is to model the underlying structure or distribution in the data in order to learn more about the data.
// MAGIC 
// MAGIC These are called unsupervised learning because unlike supervised learning above there is no correct answers and there is no teacher. Algorithms are left to their own devises to discover and present the interesting structure in the data.
// MAGIC 
// MAGIC Unsupervised learning problems can be further grouped into clustering and association problems.
// MAGIC 
// MAGIC 
// MAGIC - Clustering: A clustering problem is where you want to discover the inherent groupings in the data, such as grouping customers by purchasing behavior.
// MAGIC - Association:  An association rule learning problem is where you want to discover rules that describe large portions of your data, such as people that buy X also tend to buy Y.

// COMMAND ----------

// MAGIC %md ## K Means Clustering ##
// MAGIC K-means clustering is a type of unsupervised learning, which is used when you have unlabeled data (i.e., data without defined categories or groups). The goal of this algorithm is to find groups in the data, with the number of groups represented by the variable K. The algorithm works iteratively to assign each data point to one of K groups based on the features that are provided. Data points are clustered based on feature similarity. K-means tries to separate data points into clusters by minimizing the sum of squared errors between data points and their nearest cluster centers.
// MAGIC 
// MAGIC ![Kmeans](https://nytaxidata.blob.core.windows.net/notebookimages/clustererdvsUnclustered.png)
// MAGIC 
// MAGIC MLlib provides a Kmeans algorithm that clusters the data points into a predefined number of clusters.

// COMMAND ----------

// MAGIC %md We will use Kmeans for cluster data points on the basis of their **trip distance**, **fare**, and **duration** to see how many groupings we have within the New York Taxi Dataset.

// COMMAND ----------

// MAGIC %md We will use the `selectData` DataFrame to pick columns related to the pickup date and time and use them as features to train our K-Means model. We will use a VectorAssembler to transform these features into vector form which is required by the Kmeans algorithm.

// COMMAND ----------

val dataset=selectData.select("trip_distance","fare","duration").filter($"duration"<50000 && $"duration">0)
//we shall add a unique id to identify each individual trip
                      .withColumn("tripID",monotonically_increasing_id)
val assembler= new VectorAssembler()
                  .setInputCols(Array("trip_distance","duration","fare"))
                  .setOutputCol("features")
val assembledDataset=assembler.transform(dataset)

// COMMAND ----------

// MAGIC %md We will use MlLib's KMeans algorithm to train our model to cluster data points based on our selected features. Once our model is trained, we will apply it to our dataset to get the cluster for each data point in the prediction column. We will also use `ClusteringEvaluator` to evaluate our model.
// MAGIC 
// MAGIC For the purpose of understanding, we will assume that our pickup locations are in 3 zones/ categories. The clustering will show us which trips are part of which zone/cluster and which zone has the highest density of pickups. So, the value of K for our model will be 3. You can modify the value of K to create more clusters out of the data points and compare the results

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

// Trains a k-means model.
val kmeans = new KMeans().setK(3).setSeed(1L).setFeaturesCol("features")
val model = kmeans.fit(assembledDataset)
kmeans.explainParams

// Make predictions
val predictions = model.transform(assembledDataset)

// Evaluate clustering by computing Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictions)
println(s"Silhouette with squared euclidean distance = $silhouette")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

// COMMAND ----------

// MAGIC %md The silhouette value is a measure of how similar an object is to its own cluster (cohesion) compared to other clusters (separation). The silhouette ranges from −1 to +1, where a high value indicates that the object is well matched to its own cluster and poorly matched to neighboring clusters.
// MAGIC 
// MAGIC For our model, the silhouette value comes to be 0.73, which indicates a good matching of data points present in one cluster.

// COMMAND ----------

// MAGIC %md Using Databrick's inbuilt visualization capabilities, we can visualize clusters and plot feature grids to identify trends and correlations. Each plot in the grid corresponds to 2 features, and data points are colored by their respective cluster labels. The plots can be used to visually assess how well your data have been clustered.

// COMMAND ----------

// MAGIC %md Here, **feature0** corresponds to the **trip distance**, **feature1** corresponds to the **duration** and **feature2** corresponds to the **fare**. The line chart shows the trend across data points for that particular feature. Each scatter plots corresponds to 2 features and show the clustering based on those particular features. We can use this information to see how well our data has been clustered according to those particular features.

// COMMAND ----------

display(model, assembledDataset)

// COMMAND ----------

// MAGIC %md These are our data points in the original data. We create a scatter plot with `trip_distance`(feature 1) as x-axis and `duration`(feature 2) as y-axis

// COMMAND ----------

display(dataset.select("trip_distance","duration"))

// COMMAND ----------

// MAGIC %md Now we plot our `predictions` dataframe that with the same axes as the previous scatter plot **(trip distance** and **duration)**.The `prediction` column has the cluster numbers assigned by the Kmeans model to each data point.

// COMMAND ----------

display(predictions)
