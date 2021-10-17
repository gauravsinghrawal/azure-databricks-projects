// Databricks notebook source
// MAGIC %md 
// MAGIC # About this Tutorial #  
// MAGIC <b>Goal:</b> To help you understand the capabilities and features of Spark SQL, including how to:
// MAGIC - Create tables from DataFrame
// MAGIC - Transforming DataFrames
// MAGIC - Aggregate data using built-in Spark functions
// MAGIC - Perform SQL*joins*
// MAGIC - Tune and optimize SQL queries for improved performance. 
// MAGIC - Use the Spark UI to visualize the job processes and acquire performance insights. 
// MAGIC 
// MAGIC ##### Data: We will use the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) dataset.
// MAGIC 
// MAGIC ##### Tasks: In this tutorial you will be performing the following tasks ###  
// MAGIC 0. Convert DataFrames to tables using saveAsTable()  
// MAGIC 0. Basic Aggregation  
// MAGIC    - Using the `abs` and `round` functions to perform calculations and transformations
// MAGIC 0. Explore and implement various SQL *joins* to combine tables and DataFrames.
// MAGIC    - **Shuffle join** is the default join method in Spark SQL. Like every shuffle operation, it consists of moving data between executors.
// MAGIC    - **Broadcast join** uses broadcast variables to send DataFrames to join with other DataFrames as a broadcast variable ( e.g. only once).
// MAGIC    - Query and perform joins on data directly from SQL Data Warehouse. 
// MAGIC 0. Implement various performance and optimization techniques  
// MAGIC    - Caching interim partial results to be reused in subsequent stages. 
// MAGIC    - Manipulating the size and number of the partitions that help parallelize distributed data processing  executors.
// MAGIC 0. Explore the features and  Spark Databricks UI
// MAGIC 
// MAGIC ##### Quizzes: The tutorial includes do-it-yourself quizzes. To solve the quizzes, follow these steps:
// MAGIC 1.       Write the code in the space provided.
// MAGIC 2.       Run and test your code.
// MAGIC 3.       You can validate the accuracy by running the provided validate function.
// MAGIC 4.       If you want to see the answers, scroll to the bottom.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Two Preqrequisites:
// MAGIC 
// MAGIC The tutorial assume that the following two resources already exist. You CANNOT proceed with the lab unless these resources exist and you have the proper credentials to access them.
// MAGIC 
// MAGIC 1) The  **Azure blob storage container** that containes the NY Taxi data. You will need to configure the following parameters in **cmd 4** below.
// MAGIC 
// MAGIC 2) A properly configured **Azure SQL Datawarehouse (SQL DW)** instance. You will need to specify the **JDBC connection string** in **cmd 5** below. 
// MAGIC   
// MAGIC   <b>Note:</b> If the Azure SQL Datawarehouse instance **does not already exist**, you can create your own instance as explained in the next section.
// MAGIC   

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

//CONFIGURE AZURE STORAGE BLOB ACCESS PARAMETERS
val storageAccountName = "nytaxidata"
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="

val containerName = "taxidata"
var fileName = "NYC_Cab.csv"
val clean_trips_data = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/clean/trips_data"
val clean_fares_data = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/clean/fares_data"
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

// CONFIGURE AZURE SQL DW ACCESS HERE
// COPY the JDBC CONNECTION STRING here -- BE SURE TO REPLACE THE PASSWORD with your OWN!
val dwhJdbcUrl = """jdbc:sqlserver://azdblabsdwserver.database.windows.net:1433;database=DW1;user=labsadmin@azdblabsdwserver;password=SimplePwd12;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"""

//VERY Important: *Check and verify that the SQL DW instance is in a RESUMED state -- and not in a PAUSED state.

// COMMAND ----------

//Load the trips and fares data from the DataFrame. The data has already been cleaned and preprocessed so it can be read directly

val tripsDF=spark.read.format("csv").option("inferSchema","true").option("header","true").load(clean_trips_data)
val faresDF=spark.read.format("csv").option("inferSchema","true").option("header","true").load(clean_fares_data)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Databases and Tables / Saving Dataset As Table
// MAGIC 
// MAGIC Use the Azure Databricks **Data** menu to view the NYC taxi trip dataset in a visual manner.
// MAGIC 
// MAGIC The `DataFrameWriter.saveAsTable` method saves the content of a DataFrame as a specified table. use save mode("overwrite") api to write the tale more than once

// COMMAND ----------

tripsDF.write.mode("overwrite").saveAsTable("tripsdata_table")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Once saved, the Dataset can be viewed under the **Data** menu on the left menubar. Select the `default` database and the specified table name, i.e. `tripsdata_table`.  
// MAGIC 
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/azure-data-tripdata-table.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md ##DIY ## 
// MAGIC Write the DataFrame into a table  named `faredata_table`

// COMMAND ----------

faresDF.write.mode("overwrite").saveAsTable("faredata_table")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Basic aggregations
// MAGIC 
// MAGIC Once a table is created for our data, we can use Spark SQL to perform operations on our data.
// MAGIC There are various ways to aggregate data using <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">built-in Spark functions</a>.
// MAGIC 
// MAGIC Run the code below to compute the average of all total amounts in the `faredata_table` table.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/icon-note.webp"/> 
// MAGIC 
// MAGIC By default, you get a floating point value.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT avg(total_amount) AS averageAmount 
// MAGIC FROM faredata_table

// COMMAND ----------

// MAGIC %md
// MAGIC Convert the value to an integer using the SQL `round()` function.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT round(avg(total_amount)) AS averageAmount 
// MAGIC FROM faredata_table

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the average fare, what are the maximum and minimum fares?

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT max(total_amount) AS max, min(total_amount) AS min, round(avg(total_amount)) AS average 
// MAGIC FROM faredata_table

// COMMAND ----------

// MAGIC %md ##Use Case
// MAGIC Note that some of the fares in the `faredata_table` are negative. 
// MAGIC 
// MAGIC These figures indicate bad data. 
// MAGIC Convert all the negative amounts to positive ones and then sort the top 20 trips by their total fare.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC Create a temporary view called `FaresWithoutNegative`, where all the negative fares have been converted to positive fares.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresWithoutNegative AS
// MAGIC   SELECT tripID, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,abs(total_amount) AS total_fare
// MAGIC   FROM faredata_table

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2
// MAGIC 
// MAGIC Starting with the table `FaresWithoutNegative`, create another view called `FaresWithoutNegativeSorted` where:
// MAGIC 0. The data set has been reduced to the first 20 records.
// MAGIC 0. The records are sorted by `total_amount` in descending order.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresWithoutNegativeSorted AS
// MAGIC   SELECT * 
// MAGIC   FROM FaresWithoutNegative
// MAGIC   ORDER BY total_fare DESC
// MAGIC   LIMIT 20

// COMMAND ----------

// MAGIC %md Display the contents of the temporary view by running a simple select query on it.

// COMMAND ----------

// MAGIC %sql SELECT * from FaresWithoutNegativeSorted

// COMMAND ----------

// MAGIC %md ##DIY
// MAGIC To further refine the data, assume that all fares under $1 represent bad data and filter them out.
// MAGIC 
// MAGIC Additionally, categorize each trip's `total_fare` into $100 groups.

// COMMAND ----------

// MAGIC %md The result should look like this
// MAGIC 
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui1.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC Start with the table `FaresWithoutNegative`.
// MAGIC Create a temporary view called `FaresWithoutNegativeFiltered` where:
// MAGIC 0. The data set excludes all records where total_amount is below $1.
// MAGIC 0. The data set includes a new column called `fare100`, that should be the `total_amount` grouped by 100's. For example:
// MAGIC   * A fare of 230 should be represdented by a value of "2".
// MAGIC   * A fare of 574 should be represented report a value of "6".

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresWithoutNegativeFiltered AS
// MAGIC   SELECT *, round(total_fare / 100) AS fare100 
// MAGIC   FROM FaresWithoutNegative 
// MAGIC   WHERE total_fare >= 1;

// COMMAND ----------

// TEST - Run this cell to test your solution.
val below1 = spark.sql("select * from FaresWithoutNegativeFiltered where total_fare < 1").count()
if(below1==0){println("Validated!")} else {println("Expected Count is 0 while resultant count is"+below1+"Look into your query")}

// COMMAND ----------

// MAGIC %md  The correct implementation of the query can be found at the end of the notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Joining two tables
// MAGIC 
// MAGIC Let us correlate the data in two data sets using a SQL join. The diagram below shows dfferent types of SQL joins.
// MAGIC 
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/SQL_Join_Types-1.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md ###Shuffle Joins
// MAGIC The **Shuffle join** is the default join method in Spark SQL. Like every shuffle operation, it consists of moving data between executors. Once, finished, rows from different DataFrames are grouped in a single place according to the keys defined in the join operation. Shuffle joins move data with the same key to the one executor in order to execute specific processing.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Find the total fare earned by taxis at each pickup location###
// MAGIC 
// MAGIC 0. Use the `tripsdata_table` to create a view with the distinct pickup Location ID and tripID.
// MAGIC 0. Create another view from the `faredata_table` with the tripID and the fare amount.
// MAGIC 0. Perform a join operation on both these views to get the resulting table. 
// MAGIC 
// MAGIC Aggregations can be run on the resulting table to find the desired amount. 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW DistinctLocations AS
// MAGIC   SELECT DISTINCT PULocationID, tripID
// MAGIC   FROM tripsdata_table 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresForTrips AS
// MAGIC   SELECT *
// MAGIC   FROM faredata_table 

// COMMAND ----------

// MAGIC %md Perform a join operation with the two temporary views to generate a table correlating the fares with the pickup location. Use the column `tripID` to perform the join.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresForLocations AS
// MAGIC SELECT DistinctLocations.PULocationID, faresfortrips.total_amount 
// MAGIC FROM FaresForTrips
// MAGIC INNER JOIN DistinctLocations
// MAGIC ON DistinctLocations.TripID = FaresForTrips.TripID

// COMMAND ----------

// MAGIC %md ###### Perform a groupBy on the Location IDs and sum the fares.

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT PULocationID,SUM(total_amount) AS Sum_of_Fares
// MAGIC FROM FaresForLocations
// MAGIC GROUP BY PULocationID
// MAGIC ORDER BY Sum_of_Fares DESC

// COMMAND ----------

// MAGIC %md ###Broadcast Joins
// MAGIC Joining DataFrames can be a performance-sensitive task. After all, it involves matching data from two data sources and keeping matched results in a single place.
// MAGIC Broadcast join uses broadcast variables. Instead of grouping data from both DataFrames into a single executor (shuffle join), the broadcast join will send DataFrame to join with other DataFrame as a broadcast variable (so only once). As you can see, it's particularly useful when we know that the size of one of DataFrames is small enough to: be sent through the network and to fit in memory in a single executor. Otherwise either the cost of sending big object through the network will be costly or the OOM error can be thrown.

// COMMAND ----------

// MAGIC %md We can implement the above join operation scenario using Broadcast Join

// COMMAND ----------

// MAGIC %md
// MAGIC #### DataFrames and SQL
// MAGIC 
// MAGIC DataFrame syntax is more flexible than SQL syntax. Here we illustrate general usage patterns of SQL and DataFrames.
// MAGIC 
// MAGIC Suppose we have a data set we loaded as a table called `myTable` and an equivalent DataFrame, called `df`.
// MAGIC We have three fields/columns called `col_1` (`numeric` type), `col_2` (`string` type) and `col_3` (`timestamp` type)
// MAGIC Here are basic SQL operations and their DataFrame equivalents. 
// MAGIC 
// MAGIC Notice that columns in DataFrames are referenced by `$"<columnName>"` in Scala or using `col("<columnName>")` in Python.
// MAGIC 
// MAGIC | SQL                              | DataFrame (Scala)              | DataFrame (Python)     |
// MAGIC | -------------------------------- | ------------------------------ | ------------------------------ | 
// MAGIC | `SELECT col_1 FROM myTable`      | `df.select($"col_1")`  | `df.select(col("col_1"))`  | 
// MAGIC | `DESCRIBE myTable`               | `df.printSchema`        |  `df.printSchema()`        | 
// MAGIC | `SELECT col_1 from myTable WHERE col_1 > 0` | `df.select($"col_1").filter($"col_1" > 0)` | `df.select(col("col_1")).filter(col("col_1") > 0)` | 
// MAGIC | `..GROUP BY col_2 ORDER BY col_2` | `..groupBy($"col_2").orderBy($"col_2")` | `..groupBy(col("col_2")).orderBy(col("col_2"))` | 
// MAGIC | `..WHERE year(col_3) > 1990`  | `..filter(year($"col_3") > 1990)` | `..filter(year(col("col_3")) > 1990)` | 
// MAGIC | `SELECT * from myTable limit 10` | `df.select("*").limit(10)` | `df.select("*").limit(10)` |
// MAGIC | `display(myTable)` (text format)               | `df.show()` | `df.show()` | 
// MAGIC | `display(myTable)` (html format)        | `display(df)` | `display(df)` |
// MAGIC 
// MAGIC **Hint:** You can also run SQL queries with the special syntax `spark.sql("SELECT * FROM myTable")`

// COMMAND ----------

//Importing the necessary library functions to implement broadcast
import org.apache.spark.sql.functions.broadcast

// COMMAND ----------

//Reading our data to perform join operation
val faresForTrips=spark.sql("SELECT tripID, total_amount FROM faredata_table")
val distinctLocations=spark.sql("SELECT DISTINCT PULocationID, tripID FROM tripsdata_table")
//Broadcasting the location ID Table
val distinctLocationsBroadcasted=broadcast(distinctLocations.as("DistinctLocations"))

// COMMAND ----------

//Performing Broadcast Join
val FaresForLocations=faresForTrips.join(broadcast(distinctLocationsBroadcasted),
                                        faresForTrips.col("tripID")===distinctLocations.col("tripID"),"inner")
 

// COMMAND ----------

// Performing Group By and Sum Aggregation on the result of Broadcast Join
val sumOfFaresForLocations=spark.sql("SELECT PULocationID,SUM(total_amount) AS Sum_of_Fares FROM FaresForLocations GROUP BY PULocationID ORDER BY Sum_of_Fares DESC")

// COMMAND ----------

display(sumOfFaresForLocations)

// COMMAND ----------

// MAGIC %md #DIY
// MAGIC Now let us perform another join operation on our trips and fares dataset.
// MAGIC > Find out the average fare calculated per passenger for this complete dataset
// MAGIC 
// MAGIC We will require the `passenger_count` column from the trips dataset and the `fare_amount` column from the fares dataset.

// COMMAND ----------

// MAGIC %md The result should look like this:
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/countWithFares.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md First let us create temporary views for both the datasets. Select `tripID` and `passenger_count` from trips table and `tripID` and `fare_amount` from  fares table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --CREATE TEMP VIEW FOR RELEVENT DATA FROM tripsdata_table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --CREATE TEMP VIEW FOR RELEVENT DATA FROM faredata_table

// COMMAND ----------

// MAGIC %md First perform a shuffle join on both the temp views. Use a cross outer join for this operation. Save the result of join in a table named `CountWithFares`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- TODO
// MAGIC 
// MAGIC -- <<FILL_IN>>

// COMMAND ----------

// MAGIC %md Run the following cell after uncommenting to validate your solution.

// COMMAND ----------

//if(spark.sql("select * from CountWithFares").count.toString == "37600873725525"){println("Validated!")}else{println("There seems to be a mistake. Please look into your query.")}

// COMMAND ----------

// MAGIC %md If you are not able to get the correct result look into the solution at the end of the notebook

// COMMAND ----------

// MAGIC %md # Join Operation on data from SQL DWH

// COMMAND ----------

// MAGIC %md In the previous tutorial, we had already stored the fares data into the SQL Data Warehouse. Here we will demonstate how we can query our required data from the DWH on the fly and perform join operation with an exisiting table in Spark. The following code performs the join operation followed by the aggregation to implement the same scenario of finding the sum of fares for each pickup location.

// COMMAND ----------

//VERY Important: *Check and verify that the SQL DW instance is in a RESUMED state -- and not in a PAUSED state.
// If you get an error on this command, check that the SQL DW instance state.

//Specifythe path for storing temp data
val blobTempPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/temp"

//Creating Query to fetch data from DWH and running it to get result in onTheFlyDF
val dwhQuery: String = "Select tripID, total_amount from nyc_cab_fares_data where total_amount > 0"
val onTheFlyDF: org.apache.spark.sql.DataFrame = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url",dwhJdbcUrl)
  .option("tempdir", blobTempPath)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("query", dwhQuery)
  .load()
onTheFlyDF.createOrReplaceTempView("fares_table")

//Performing join operation between data from DWH and Location table stored in Spark
val joinDF=spark.sql("SELECT DistinctLocations.PULocationID, fares_table.total_amount FROM fares_table INNER JOIN DistinctLocations ON DistinctLocations.TripID = fares_table.TripID")
joinDF.createOrReplaceTempView("location_fare_table")
//Performing group by and sum aggregation
val faresForLocationDF=spark.sql("select PULocationID, sum(total_amount) as total_fare from location_fare_table group by PULocationID order by total_fare")

// COMMAND ----------

//RUN THIS CELL TO SEE THE RESULT OF THE PREVIOUS CELL
display(faresForLocationDF)

// COMMAND ----------

// MAGIC %md ## PERFORMANCE TUNING AND OPTIMIZATIONS

// COMMAND ----------

// MAGIC %md ##Caching
// MAGIC Caching or persistence are optimisation techniques for (iterative and interactive) Spark computations. They save interim partial results so they can be reused in subsequent stages. These interim results are RDDs are thus kept in memory (default) or more solid storages like disk and/or replicated.
// MAGIC 
// MAGIC Spark SQL can cache tables using an in-memory columnar format by calling spark.catalog.cacheTable("tableName") or dataFrame.cache(). Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure. You can call spark.catalog.uncacheTable("tableName") to remove the table from memory.
// MAGIC 
// MAGIC NOTE: Data is cached in memory only after it is pulled into it. So, caching only works if an action is performed after. Normally, we perform a count or foreach operation to cache data

// COMMAND ----------

tripsDF.cache()
tripsDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC TripsDF is cached in memory now. To clear the dataFrame from the memory, use the .unpersist() command

// COMMAND ----------

tripsDF.unpersist()

// COMMAND ----------

// MAGIC %md ### Let us now see how caching affects the performance using a simple use case.

// COMMAND ----------

// MAGIC %md We will use the tripsDF DataFrame to demostrate the benefits in performance of caching. Our objective is to calculate the sum of all trip distances according to the different payment types and the store & fwd flag. The condition is that it should exclude all trips less than 2 miles.

// COMMAND ----------

// MAGIC %md First let us implement this scenario without caching

// COMMAND ----------

import org.apache.spark.sql.functions._
val distanceSumDF=tripsDF.select("trip_distance","payment_type","store_and_fwd_flag")
        .filter($"trip_distance">2)
        .groupBy("payment_type","store_and_fwd_flag")
        .agg(sum("trip_distance"))

// COMMAND ----------

// MAGIC %md Calling the show() action will trigger the processing of the above cell.

// COMMAND ----------

distanceSumDF.show

// COMMAND ----------

// MAGIC %md Now, let us cache the intermediate result in-memory for faster and better processing. We will perform the select and filter operation and cache their result in-memory. We will call a count() to trigger the caching operation. 

// COMMAND ----------

val tripsCached=tripsDF.select("trip_distance","payment_type","store_and_fwd_flag")
                       .filter($"trip_distance">2)
                       .cache()
tripsCached.count()

// COMMAND ----------

// MAGIC %md Now, Let us perform the group by and aggregation on the cached DataFrame

// COMMAND ----------

val distanceSumDF1=tripsCached.groupBy("payment_type","store_and_fwd_flag")
            .agg(sum("trip_distance"))

// COMMAND ----------

distanceSumDF1.show

// COMMAND ----------

// MAGIC %md You can see that the caching makes the query performance better and we get the result in less time.
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/caching.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md ## SPARK UI

// COMMAND ----------

// MAGIC %md To make the process of understanding performance improvements more explicit and for better understanding of Spark internals, let us explore the Spark UI that is provided in Databricks.
// MAGIC 
// MAGIC To access it go to Clusters>Your Cluster then click on Spark UI tab from the top.

// COMMAND ----------

// MAGIC %md To get a more comprehensive look into the Spark UI, we recommend this blog post by Databricks: [Understanding your Apache Spark Application Through Visualization](https://databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html)

// COMMAND ----------

// MAGIC %md The Spark UI displays events in a timeline such that the relative ordering and interleaving of the events are evident at a glance.
// MAGIC 
// MAGIC The timeline view is available on three levels: across all jobs, within one job, and within one stage. On the landing page, the timeline displays all Spark events in an application across all jobs.
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui1.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md Let us look into the Spark UI to see how our last job was executed both with and without caching.

// COMMAND ----------

// MAGIC %md These are the jobs that were running in our Spark cluster when we executed the show command for `distanceSumDF`, i.e without caching. 
// MAGIC 
// MAGIC We can see the list of jobs executed and the time taken for execution for each job. It also shows the tasks completed and skipped by Spark while executing each job.
// MAGIC 
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui2.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md 
// MAGIC On clicking on a particular job ID, we can see the stages completed as part of that job along with the DAG visualization. We can see from the DAG that data was fetched from the beginning for each stage. This process can be optimized by caching the DataFrame.
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui3.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md 
// MAGIC This Spark UI shows us the jobs executed when we call the show action for `distanceSumDF1`, i.e. the one where we cached the data in-memory. You can notice that the job was executed in much less time than in the previous case.
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui4.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md On clicking on the command number in the description tab, we can see the DAG visualization as well as the stages executed for each job. The DAG for caching scenario is different than the one where data was not cached. 
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/sparkui5.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md ## Partitioning ##
// MAGIC 
// MAGIC Spark manages data using partitions that helps parallelize distributed data processing with minimal network traffic for sending data between executors. Data
// MAGIC Frames get partitioned automatically without programmer intervention. However, there are times when you’d like to adjust the size and number of partitions or the partitioning scheme according to the needs of your application.

// COMMAND ----------

// MAGIC %md We can use the getNumPartitions() function to get the number of partitions of our DataFrame.
// MAGIC 
// MAGIC NOTE: getNumPartitions() is a function of RDD class so we need to convert our DataFrame to RDD first to apply this function. DataFrames can be converted to RDDs by simply applying .rdd infront of the dataframe.

// COMMAND ----------

tripsDF.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md We can repartition data to increase the level of parallelism to optimize our job.
// MAGIC 
// MAGIC The repartition method can be used to either increase or decrease the number of partitions in a DataFrame.

// COMMAND ----------

//repartition to 10000
val repartitionTripsDF1=tripsDF.repartition(10000)
println("Repartition size="+repartitionTripsDF1.rdd.getNumPartitions)
val count1=repartitionTripsDF1.select("passenger_count").count

// COMMAND ----------

// MAGIC %md View number of partition in spark UI
// MAGIC 
// MAGIC <img src="https://nytaxidata.blob.core.windows.net/notebookimages/partition1000.png" style="border: 10px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

//repartition to 8
val repartitionTripsDF2=tripsDF.repartition(8)
println("Repartition size="+repartitionTripsDF2.rdd.getNumPartitions)
val count2=repartitionTripsDF2.select("passenger_count").count

// COMMAND ----------

//repartition to 1
val repartitionTripsDF3=tripsDF.repartition(1)
println("Repartition size="+repartitionTripsDF3.rdd.getNumPartitions)
val count3=repartitionTripsDF3.select($"passenger_count").count

// COMMAND ----------

// MAGIC %md In general, smaller/more numerous partitions allow work to be distributed among more workers, but larger/fewer partitions allow work to be done in larger chunks, which may result in the work getting done more quickly as long as all workers are kept busy, due to reduced overhead.
// MAGIC Spark can only run 1 concurrent task for every partition of an RDD or Dataframe, up to the number of cores in your cluster. So if you have a cluster with 50 cores, you want your RDDs or Dataframes to at least have 50 partitions (and probably 2-3x times that). The maximum size of a partition is ultimately limited by the available memory of an executor.

// COMMAND ----------

// MAGIC %md As we can observe above three cells time taken by  10000 partitions > 1 partition > 8 partition. This means that
// MAGIC  increasing or decreasing number of partition can optimize or can worsen the time. So we must choose wisely number of partition. When number of partitions are more than required it creates some empty partitions and certainly processing those partition consumes performance. When we process less number of partitons than required then parallelism decreases and hence the performance.  
// MAGIC  ###The optimal number of partitions is dependent on various factors like size of data, cluster size and configurations, no. of cores and executors

// COMMAND ----------

// MAGIC %md
// MAGIC It’s critical to partition wisely in order to manage memory pressure as well as to ensure complete resource utilization on executor’s nodes. You must always know your data—size, types, and how it’s distributed. A couple of best practices to remember are:
// MAGIC 
// MAGIC - Understanding and selecting the right operators for actions like reduceByKey or aggregateByKey so that your driver is not put under pressure and the tasks are properly executed on executors.
// MAGIC 
// MAGIC - If your data arrives in a few large unsplittable files, the partitioning dictated by the InputFormat might place large numbers of records in each partition, while not generating enough partitions to take advantage of all the available cores. In this case, invoking repartition with a high number of partitions after loading the data will allow the operations that come after it to leverage more of the cluster’s CPU.
// MAGIC 
// MAGIC - Also, if data is skewed then repartitioning using an appropriate key which can spread the load evenly is also recommended.

// COMMAND ----------

// MAGIC %md The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data.
// MAGIC 
// MAGIC Coalesce combines existing partitions to avoid a full shuffle.
// MAGIC 
// MAGIC NOTE: Coalesce function can be used only to decrease the number of partitions. Using coalesce to increase the number of partitions will cause no change in the number of partitions.

// COMMAND ----------

//We can use coalsesce instead of repartition because it avoids shuffle
val coalesceTripsDF=tripsDF.coalesce(1)
println("coalesce size="+coalesceTripsDF.rdd.getNumPartitions)
val count2=coalesceTripsDF.select($"passenger_count").count

// COMMAND ----------

// MAGIC %md The partitioning of DataFrames seems like a low level implementation detail that should be managed by the framework, but it’s not. When filtering large DataFrames into smaller ones, you should almost always repartition the data.

// COMMAND ----------

// MAGIC %md ## SOLUTIONS ##

// COMMAND ----------

//DIY1
faresDF.write.mode("overwrite").saveAsTable("faredata_table")

// COMMAND ----------

// MAGIC %sql --DIY2
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FaresWithoutNegativeFiltered AS
// MAGIC   SELECT *, round(total_fare / 100) AS fare100 
// MAGIC   FROM FaresWithoutNegative 
// MAGIC   WHERE total_fare >= 1;

// COMMAND ----------

// MAGIC %sql --DIY3
// MAGIC create or replace temporary view CountOfPassengers as
// MAGIC select tripID, passenger_count from tripsdata_table;

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temporary view FaresForPassenger as
// MAGIC select tripID, fare_amount from faredata_table;

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temporary view CountWithFares as
// MAGIC select CountOfPassengers.passenger_count, FaresForPassenger.fare_amount
// MAGIC from CountOfPassengers
// MAGIC cross join FaresForPassenger;
