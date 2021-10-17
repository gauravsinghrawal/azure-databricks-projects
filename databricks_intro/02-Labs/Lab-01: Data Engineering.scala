// Databricks notebook source
// MAGIC %md
// MAGIC ## About this Tutorial
// MAGIC 
// MAGIC #####Goal: The goal of the tutorial is to help you understand how Azure Databricks Spark can be used to prepare raw data for analytics.  You will learn the following typical data preparation concepts:
// MAGIC 
// MAGIC - Exploring and understanding the data--including the fields, schema, size--by visualizing the data in both table and graph form.
// MAGIC - Identifying outliers and techniques to mitigate their impact
// MAGIC - Finding and removing null values from datasets
// MAGIC - Converting and formatting data.
// MAGIC 
// MAGIC  
// MAGIC #####Learnings: You will learn about the following capabilities of Spark to achieve these goals:
// MAGIC 
// MAGIC 1.       Dataframes: a distributed collection of data organized into named columns. They are derived from Resilient Distributed Datasets (RDDs), the core Spark data structure, and are conceptually equivalent to a table in a relational database.
// MAGIC 
// MAGIC 2.       Spark functions including:
// MAGIC   - Filter
// MAGIC   - Display
// MAGIC   - Show
// MAGIC   - Cast
// MAGIC   - Count
// MAGIC   - withColumn
// MAGIC 
// MAGIC #####Data: This tutorial will use the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) dataset.  
// MAGIC 
// MAGIC 
// MAGIC #####Tasks: In this tutorial you will be performing the following major tasks:
// MAGIC 1.       Exploring and visualizing the data
// MAGIC   *        Using printSchema(), show(), count() and display() functions
// MAGIC   *        Using Spark's built-in charting capabilities
// MAGIC 2.       Converting data types
// MAGIC   * Using cast() function
// MAGIC 3.       Detecting and handling outliers
// MAGIC 4.       Null detection and handling
// MAGIC 5.       Formatting  Data
// MAGIC   * Using standard date functions
// MAGIC 
// MAGIC 
// MAGIC #####DIY Quizzes: The tutorial includes do-it-yourself quizzes. To solve the quizzes, follow these steps:
// MAGIC 1.       Write the code in the space provided.
// MAGIC 2.       Run and test your code.
// MAGIC 3.       You can validate the accuracy of your answers by running the provided *validate* function.
// MAGIC 4.       If you want to see the answers, you can scroll to the bottom where they are all located.  

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Two Preqrequisites:
// MAGIC 
// MAGIC The tutorial assumes that the following two resources already exist. You CANNOT proceed with the lab unless these resources exist.
// MAGIC 
// MAGIC 1) The  **Azure blob storage container** that containes the NY Taxi data. You will need to configure the connection parameters in `cmd 4` below.
// MAGIC 
// MAGIC 2) A properly configured **Azure SQL Datawarehouse (SQL DW)** instance. You will need to specify the **JDBC connection string** in `cmd5` below.
// MAGIC   
// MAGIC   <b>Note:</b> If the Azure SQL Datawarehouse instance **does not already exist**, you can create your own instance as explained in the next section.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Instructions to provision your own Azure SQL DW Instance (if one does not already exist)
// MAGIC 
// MAGIC   
// MAGIC   1) Create an Azure SQL DW instance using [Azure PowerShell](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/create-data-warehouse-powershell) or the [Azure Portal](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/create-data-warehouse-portal#create-a-data-warehouse) 
// MAGIC   
// MAGIC   2) Then create a *database master key*. The database master key is required so we can access the Azure SQL DW from within Databricks. The key is the password provided while creating the Data Warehouse instance.  To do this, go to the query editor of your DW instance and run the following query :
// MAGIC   
// MAGIC   **`CREATE MASTER KEY ENCRYPTION BY PASSWORD = ‘<your SQL DW instance password>'`**
// MAGIC     
// MAGIC     
// MAGIC   ![DWH1](https://i.postimg.cc/V60fhxGS/dwh-master-key.png)  
// MAGIC   
// MAGIC   <br/>

// COMMAND ----------

//CONFIGURE AZURE STORAGE BLOB ACCESS PARAMETERS HERE

val storageAccountName = "nytaxidata"
val containerName = "taxidata"
val fileName = "NYC_Cab.csv"
val blobname = "NYC_fares" 
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="
val dwhFilePath = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${blobname}"
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

// CONFIGURE AZURE SQL DW ACCESS HERE
// If you created your own SQL DW instance, then replace the JDBC connection string with the one for your instance.

val dwhJdbcUrl = """jdbc:sqlserver://azdblabsdwserver.database.windows.net:1433;database=DW1;user=labsadmin@azdblabsdwserver;password=SimplePwd12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"""

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Copy data from Azure Blob Container to Azure SQL DWH
// MAGIC 
// MAGIC Run the following cell to push `nyc_cab_fares_data` to your Azure SQL DWH Instance.   
// MAGIC 
// MAGIC Later (in `cmd 14`) we will fetch this data again into Databricks and perform transformations on it. Learn more about connecting Azure Databricks to Azure SQL DW [here](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/sql-data-warehouse.html)
// MAGIC 
// MAGIC **VERY Important:** *Check and verify that the SQL DW instance is in a `RESUMED` state -- and not in a `PAUSED` state.

// COMMAND ----------

import org.apache.spark.sql.SaveMode 
import com.databricks.spark.sqldw.SqlDWSideException
// READ FARES DATA FROM BLOB

val nycFaresPushtoDW = spark.read.format("csv").option("header","true").option("inferSchema","true").load(dwhFilePath)
val tablename = "nyc_cab_fares_data"
// WRITE DF TO DWH IN TABLE
try
{
  nycFaresPushtoDW.write
    .format("com.databricks.spark.sqldw")
    .mode(SaveMode.Ignore)
    .option("url", dwhJdbcUrl)
    .option("forwardSparkAzureStorageCredentials", "true")
    .option("dbTable", tablename)
    .option("tempDir", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/tmp")
    .save()
}
catch {
  case ex: SqlDWSideException => { ex match {
            case x if ex.getMessage.contains("paused") => println("Connection failed as DW Instance is PAUSED. Please RESUME it to connect.")
            case x if ex.getMessage.contains("TCP/IP connection") => println("Connection failed due to error in connection string. Check your connection string and try again.")
  } } }

// COMMAND ----------

// MAGIC %md 
// MAGIC ### DATASET: New York City Taxi Trips 
// MAGIC This dataset contains detailed trip-level data on New York City Taxi trips. It was collected from both driver inputs and GPS coordinates of individual cabs. 
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

// MAGIC %md
// MAGIC 
// MAGIC ### 1. Reading data from Blob Storage###
// MAGIC 
// MAGIC The first step in our data analysis is to load the dataset into Azure Databricks.
// MAGIC 
// MAGIC Since the data set is in CSV format, use the `spark.read.csv` operation. Spark SQL supports other formats, e.g. `text`, `json`, `jdbc`, `parquet`, `orc`. *You could also create your own if so desire.*
// MAGIC 
// MAGIC CSV files have a `header` option which controls whether the first line of every *(part) file of a* CSV file should be specified as  the header of the dataset. Use the `option` method to specify the option name (`header`) and the value (`true` or `false`).
// MAGIC 
// MAGIC This dataset has a header so let's tell Spark to use it for column names.
// MAGIC 
// MAGIC Other options include:
// MAGIC 
// MAGIC 1. `inferSchema` to infer the schema of the dataset
// MAGIC 1. `columnNameOfCorruptRecord` for the column name with records that Spark could not load for whatever reason
// MAGIC 1. `dateFormat` that defaults to `yyyy-MM-dd`
// MAGIC 1. `delimiter`
// MAGIC 1. `timestampFormat` with the default value of `yyyy-MM-dd’T’HH:mm:ss.SSSXXX`
// MAGIC 1. `quote` that controls what character to use for quotes (default: `\"`)
// MAGIC 
// MAGIC The list of available options is format-specific. Read the official documentation of Spark SQL to find out more on the options.

// COMMAND ----------

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException
var dataFromBlob : org.apache.spark.sql.DataFrame = null
try {
   //importing data from blob
   val blobFilePath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/${fileName}"
   dataFromBlob= spark.read.option("header","true").csv(blobFilePath)
  
}catch {
  case ex: IOException => {
            println("Invalid Blob Connection")} 
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ###2. View dataset schema###

// COMMAND ----------

dataFromBlob.printSchema()

// COMMAND ----------

// MAGIC %md 
// MAGIC ###3. Read data from SQL Data Warehouse using JDBC###

// COMMAND ----------

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

var dataFromDWH : org.apache.spark.sql.DataFrame = null
try {
  
   //importing data from dataware house
  dataFromDWH = spark.read
                .format("com.databricks.spark.sqldw")
                .option("url", dwhJdbcUrl)
                .option("tempDir", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/tmp")
                .option("forwardSparkAzureStorageCredentials", "true")
                .option("dbTable", "nyc_cab_fares_data")
                .load()

}catch {
  case ex: IOException => {
            println("Invalid DataWare House Connection")} 
}

// COMMAND ----------

// MAGIC %md
// MAGIC ###4. View schema from SQL Data Warehouse###

// COMMAND ----------

dataFromDWH.printSchema

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 5. Use the *count* action to get the number of records###

// COMMAND ----------

val blobCount=dataFromBlob.count
val dwhCount=dataFromDWH.count

// COMMAND ----------

// MAGIC %md ### Exploratory Data Analysis

// COMMAND ----------

// MAGIC %md Using the `show` method to view the contents of the DataFrames from Blob and Data Warehouse, we can infer that the data read from Blob is related to the trip details ( i.e. # of passengers, distances, time, etc.) while the data from DWH concerns the fares and surcharges.

// COMMAND ----------

// VIEW SAMPLE DATA FOR TRIPS
val tripsDF=dataFromBlob
tripsDF.show(10)

// COMMAND ----------

//VIEW SAMPLE DATA FOR FARES
val faresDF=dataFromDWH
faresDF.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Type conversions / Converting string literals into doubles
// MAGIC 
// MAGIC 
// MAGIC The schema of the DataFrame containing trip "metadata" shows that all the fields have the the data type `string`, which isn't always appropriate. For example, `passenger_count` and `tpep_dropoff_datetime` should be `int` and `timestamp` respectively.
// MAGIC 
// MAGIC Before proceeding further it is important to ensure all the fields are of the correct data type.
// MAGIC 
// MAGIC `Column.cast(to: String)` method casts a column to a different data type, using the canonical string representation of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`, `float`, `double`, `decimal`, `date`, `timestamp`.

// COMMAND ----------

//Perform a select query with cast function on particular columns
import org.apache.spark.sql.types._
val tripsCastDF=tripsDF.select($"tripID",
                               $"VendorID",
                               $"tpep_pickup_datetime".cast(TimestampType),
                               $"tpep_dropoff_datetime".cast(TimestampType),
                               $"passenger_count".cast(IntegerType),
                               $"trip_distance".cast(DoubleType),
                               $"RatecodeID",
                               $"store_and_fwd_flag",
                               $"PULocationID",
                               $"DOLocationID",
                               $"payment_type".cast(IntegerType))

// COMMAND ----------

// MAGIC %md View the schema for trips DataFrame again after the typecasting operation

// COMMAND ----------

tripsCastDF.printSchema

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #Visualization
// MAGIC 
// MAGIC Databricks provides easy-to-use, built-in visualizations for your data. 
// MAGIC 
// MAGIC Display the data by using the Databricks Spark `display` function.
// MAGIC 
// MAGIC Visualize the query below by selecting the bar graph icon once the table is displayed:
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/visualization-1.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/
// MAGIC >
// MAGIC 
// MAGIC Let's visualise the trip and fares DataFrame to better understand the data.

// COMMAND ----------

display(faresDF)

// COMMAND ----------

// MAGIC %md ## Outlier detection and handling ##
// MAGIC 
// MAGIC The line chart of the "fares" DataFrame shows several trips for which  `fare_amount` is $600-700, which is implausible in the real world. These data points are considered outliers; they are highly abnormal values. There are several ways to calculate what values constitute an outlier. In this scenario, a normal fare amount is defined as between $0-100. Any amount above $100 will be considered an outlier. One way of managing outliers is replacing the aberrant values with the average value of the data points across the entire dataset. 

// COMMAND ----------

import org.apache.spark.sql.functions._
//Filter the data to get rows without the outliers
val filteredFaresDF=faresDF.filter($"fare_amount"<=100)
//Calculate average fare for all valid fares
val mean=filteredFaresDF.agg(avg($"fare_amount")).first.getDouble(0)
//Insert average fare in place of outliers
val correctFaresDF=faresDF.select($"*", when($"fare_amount">100, mean).otherwise($"fare_amount").alias("fare"))
                          .drop($"fare_amount").withColumnRenamed("fare","fare_amount")

// COMMAND ----------

// MAGIC   %md  Since the `fare_amount` has been modified for several trips, all fields that are dependent on `fare_amount` need to be recalculated. In this scenario, `total_amount` is the sum of the base `fare_amount` and various other surcharges(tolls, tips etc.)

// COMMAND ----------

//Total amount as a sum of fare amount, extra, MTQ, Tips, Tolls and Improvement surcharge
val correctTotalFaresDF=correctFaresDF.withColumn("total",$"fare_amount"+$"extra"+$"mta_tax"+$"tip_amount"+$"tolls_amount"+$"tip_amount"+$"improvement_surcharge").drop("total_amount").withColumnRenamed("total","total_amount")

// COMMAND ----------

// MAGIC %md After removing the outliers, a visualization of the dataset should show no egregious outliers. 

// COMMAND ----------

display(correctTotalFaresDF)

// COMMAND ----------

// MAGIC %md ## Writing Corrected Data Back to SQL Data Warehouse
// MAGIC Once the data has been cleaned, it's ready to be pushed back to SQL Data Warehouse, where it can be stored for future access. Use the same JDBC Connection String from before to write the DataFrame back to DWH.
// MAGIC 
// MAGIC **NOTE:** *For this tutorial the number of records is limited to 100 as writing to SQL Data Warehouse is a time-intensive process. Refer to Azure Databricks Docs to learn more. If you want to test the code for the complete load, remove the limit method and run the query.*
// MAGIC 
// MAGIC use .mode("overwrite") if you want to overwrite table more than once.

// COMMAND ----------

correctTotalFaresDF
  .limit(100)
   .write
  .mode("overwrite")
  .format("com.databricks.spark.sqldw")
  .option("url", dwhJdbcUrl)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "nyc_cab_fares_clean")
  .option("tempDir", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/tmp")
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC **NOTE:** At the end of the lab, dont forget to pause your Azure SQL Data Warehouse instance.

// COMMAND ----------

// MAGIC %md #Data Cleaning and Preprocessing

// COMMAND ----------

// MAGIC %md Let's visualize our trips data and examine the pickup and dropoff times

// COMMAND ----------

display(tripsCastDF)

// COMMAND ----------

// MAGIC %md The bar chart of the trips DataFrame indicates missing points and gaps in the pickup times and passenger counts. Performing a simple filter operation to check for null and corrupt records will help clarify the cause of these irregularities. 

// COMMAND ----------

//Filtering for records that have pickup and drop time as null
val nullRecordsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNull or $"tpep_dropoff_datetime".isNull)
val nullRecordsCount=nullRecordsDF.count

// COMMAND ----------

// MAGIC %md The filters indicate there are a signficant number of records where the pickup/dropoff time is null. Dealing with null data can vary from case to case. In this tutorial, we will consider these null values to be corrupt. The data cleaning process will consist of removing these corrupt records.

// COMMAND ----------

//Filtering out records where pickup and dropoff date is null
val filterTripsDF=tripsCastDF.filter($"tpep_pickup_datetime".isNotNull or $"tpep_dropoff_datetime".isNotNull)

// COMMAND ----------

// MAGIC %md ##DIY: Perform cleaning on this dataset for records where passenger count is 0

// COMMAND ----------

// MAGIC %md Visualize the data again and display the passenger count.

// COMMAND ----------

display(filterTripsDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC Filter and count the number of corrupt records. Corrupt records are the ones where `passenger_count` is equal to 0

// COMMAND ----------

//Filtering for records that have passenger count as zero
val zeroPassengersDF=filterTripsDF.filter($"passenger_count"===0)
val zeroPassengersCount=zeroPassengersDF.count

// COMMAND ----------

// MAGIC %md Now, generate the DataFrame `filteredTripsDF` that does not have these corrupt records.

// COMMAND ----------

val filteredTripsDF=filterTripsDF.filter($"passenger_count"=!=0)

// COMMAND ----------

// MAGIC %md Validate your process by counting the number of clean and corrupt records. The total records count should match the initial DataFrame count

// COMMAND ----------

val cleanRecordsCount=filteredTripsDF.count
val totalRecordsCount=cleanRecordsCount+zeroPassengersCount+nullRecordsCount

// COMMAND ----------

// MAGIC %md Run the following command to test your solution

// COMMAND ----------

 if (totalRecordsCount == blobCount) "Validated!" else "No"

// COMMAND ----------

// MAGIC %md If you are not able to get the correct result you can go to `Cmd 84` and `85` at the end of the notebook to view the answers

// COMMAND ----------

//RUN THIS CELL TO LOAD THE VALIDATION RESULTS
val resultFromBlob= spark.read.option("header","true").csv(s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/nycTaxiResult.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Performing Operations on the Data &mdash; Formatting

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Once a dataset has been loaded, it's common to perform some preprocessing before proceeding to analyze it. 
// MAGIC 
// MAGIC Spark SQL's Dataset API comes with a library called [standard functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) that allow for data manipulation. These, as well as  column-based operators like `select` or `withColumn`, are tools for performing the necessary transformations for data analysis.

// COMMAND ----------

// MAGIC %md ## Use Case: Categorize Users by *time of day* when Trip is Taken
// MAGIC Extract the Hour of Day from the timestamp and categorize them into 
// MAGIC - Trips taken between 0000 hours to 1200 hours.
// MAGIC - Trips taken between 1200 hours to 2400 hours.
// MAGIC We will use in-built Spark functions to filter the Months and Hours and then perform a groupBy operation to get a count of users across 4 hour intervals throughout the day.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Built-In Functions
// MAGIC 
// MAGIC Spark provides a number of <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>, many of which can be used directly with DataFrames.  Use these functions in the `filter` expressions to filter data and in `select` expressions to create derived columns.
// MAGIC 
// MAGIC 
// MAGIC #### Splitting timestamps into separate columns with date, year and month
// MAGIC 
// MAGIC The standard functions for working with dates:
// MAGIC 
// MAGIC * `dayofmonth`
// MAGIC * `year`
// MAGIC * `weekofyear`
// MAGIC * `quarter`
// MAGIC * `month`
// MAGIC * `minute`

// COMMAND ----------

// Using Inbuilt functions to extract Hours from timestamp and categorizing them according to quarters
import org.apache.spark.sql.functions._
val timeCategoryDF=filteredTripsDF.withColumn("tripHour", hour(col("tpep_pickup_datetime")))
                                  .withColumn("quarterlyTime", when($"tripHour".geq(lit(0)) and $"tripHour".lt(lit(4)),"0to4").when($"tripHour".geq(lit(4)) and $"tripHour".lt(lit(8)),"4to8").when($"tripHour".geq(lit(8)) and $"tripHour".lt(lit(12)),"8to12").when($"tripHour".geq(lit(12)) and $"tripHour".lt(lit(16)),"12to16").when($"tripHour".geq(lit(16)) and $"tripHour".lt(lit(20)),"16to20").when($"tripHour".geq(lit(20)) and $"tripHour".lt(lit(24)),"16to24"))

// COMMAND ----------

val countByTimeOfDayDF=timeCategoryDF.groupBy("quarterlyTime").count

// COMMAND ----------

// MAGIC %md To better understand the data, visualize the results. 

// COMMAND ----------

display(countByTimeOfDayDF)

// COMMAND ----------

// MAGIC %md ## DIY: Extract Day of Month from Timestamp and append to DataFrame as seperate column having name `DateOfTrip`

// COMMAND ----------

// MAGIC %md ###### Fill the commented //TO-FILL with your query and run the cell and then validate the result running by next cell 
// MAGIC for example : val tripsWithDF= //TO-FILL  to  val tripsWithDF= trips.select("tripID")

// COMMAND ----------

// MAGIC %md Use the Spark `withColumn` function to add a new column to our Trips DataFrame. The values for the new column are to be extracted from the `tpep_pickup_datetime` column. Use the inbuilt Spark functions to extract the date of month from the timestamp value.

// COMMAND ----------

// DBTITLE 1,The resulting DataFrame should look like this with an added DateOfTrip column
// MAGIC %md
// MAGIC ![wc](https://i.ibb.co/wzZ7sx4/dfwithcolumn.png)

// COMMAND ----------

// MAGIC %md HINT: We can use in-built Spark functions to extract day of month from a column of type Timestamp. 

// COMMAND ----------

def extractDayDF(): org.apache.spark.sql.DataFrame ={  
   
val tripsWithDayOfMonthDF= filteredTripsDF.withColumn("DateOfTrip",dayofmonth($"tpep_pickup_datetime"))
    
   return(tripsWithDayOfMonthDF);
}

// COMMAND ----------

// MAGIC %md Run the following to validate your solution

// COMMAND ----------

if (resultFromBlob.select($"dayOfMonth").head.getString(0).toLong ==  extractDayDF().agg(sum($"DateOfTrip")).head.getLong(0) )
   { println("Validated!"); }
else
   { println("The result is not correct. Please look into your query"); }

// COMMAND ----------

// MAGIC %md If you can't get the correct result, you can go to `Cmd 84` and `85` at the end of the notebook to view the answers

// COMMAND ----------

// MAGIC %md ## Use Case: Filter Operation to get Data for a particular vendor

// COMMAND ----------

// MAGIC %md Filtering is an important and powerful functionality to understand. Spark operations can be highly optimized by filtering out unwanted data.
// MAGIC 
// MAGIC Spark's Dataset API has the methods `filter` & `where` which filter rows using a given condition. A condition can be written either as `func: (T) ⇒ Boolean` or as a SQL or `Column` expression.
// MAGIC 
// MAGIC The `Column` expression is computed based on the data in a DataFrame.

// COMMAND ----------

//filter the record of vendor id 2
val tripsForVendorDF = filteredTripsDF.filter($"VendorID" === 2)

// COMMAND ----------

display(tripsForVendorDF)

// COMMAND ----------

// MAGIC %md ## DIY: Use Filter to get location for a particular Pickup Location and count number of trips for that location

// COMMAND ----------

// MAGIC %md First, filter the `PULocationID` column to get only those trips having Location ID 132. Then perform a count operation to get the number of trips from that particular location.

// COMMAND ----------

def filterForLocation(): Long ={ 
       val filterForLocationDF=filteredTripsDF.filter($"PULocationID"===132)
  
  
  
       val noOfTripsForLocation=filterForLocationDF.count
       
  
  
       return(noOfTripsForLocation);
}

// COMMAND ----------

// MAGIC %md Run the following command to validate your solution

// COMMAND ----------

if (resultFromBlob.select($"filterForLocationCount").head.getString(0).toLong == filterForLocation())
   { println("Validated!"); }
else
   { println("The result seems to be incorrect. Please look into your solution"); }

// COMMAND ----------

// MAGIC %md If you are not able to get the correct result you can go to `Cmd 100` at the end of the notebook to view the answers

// COMMAND ----------

// MAGIC %md ## Use Case: Calculate Average Speed Distribution per trip
// MAGIC Calculate the Average Speed for each trip by finding the duration of each trip; this can be done by calculating the difference of pickup time and the dropoff time. Then divide the `distance` column by the duration to get the mean speed.

// COMMAND ----------

val calcTripDurationDF=filteredTripsDF.withColumn("duration", (unix_timestamp($"tpep_dropoff_datetime") - unix_timestamp($"tpep_pickup_datetime"))/3600)
val calcAvgSpeedDF=calcTripDurationDF.withColumn("speed",$"trip_distance"/ $"duration")
display(calcAvgSpeedDF.select($"tripID",$"speed"))

// COMMAND ----------

// MAGIC %md ## DIY: Find the payment type being used by maximum people

// COMMAND ----------

// MAGIC %md Use the `payment_type` column of the fares DataFrame to get a count of the usage of various payment methods. Then sort them to find out which payment methods are being used more frequently.

// COMMAND ----------

def orderPaymentMethodsDF(): org.apache.spark.sql.DataFrame ={  
  
  val eachPaymentCountDF=filteredTripsDF.groupBy($"payment_type").count

  
  
  val paymentIdHighestToLowestDF=eachPaymentCountDF.orderBy("count")
  
  
  
  return(paymentIdHighestToLowestDF);
}

// COMMAND ----------

// MAGIC %md Run the following cell to check your solution

// COMMAND ----------

if (resultFromBlob.select($"paymentType").head.getString(0).toLong ==  orderPaymentMethodsDF().select($"count").head.getLong(0) )
   { println("Validated"); }
else
   { println("There seems to be a mistake. Please look into your query."); }

// COMMAND ----------

// MAGIC %md If you get an incorrect result you can go to `Cmd 100` at the end of the notebook to view the solution

// COMMAND ----------

// MAGIC %md ## Results of DIY Quiz

// COMMAND ----------

//DIY0
val zeroPassengersDF=filterTripsDF.filter($"passenger_count"===0)
val zeroPassengersCount=zeroPassengersDF.count

// COMMAND ----------

val filteredTripsDF=filterTripsDF.filter($"passenger_count" =!= 0)

// COMMAND ----------

//DIY1
val filterForLocationDF=filteredTripsDF.filter($"PULocationID"==="132")
val noOfTripsForLocation=filterForLocationDF.count

// COMMAND ----------

//DIY2
val tripsWithDayOfMonthDF= filteredTripsDF.withColumn("DateOfTrip",dayofmonth($"tpep_pickup_datetime")) 

// COMMAND ----------

//DIY3
val eachPaymentCountDF=filteredTripsDF.groupBy($"payment_type").count
val paymentIdHighestToLowestDF=eachPaymentCountDF.orderBy("count")
