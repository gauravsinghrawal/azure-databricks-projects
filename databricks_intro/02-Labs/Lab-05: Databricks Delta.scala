// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ## About this Tutorial ##  
// MAGIC 
// MAGIC #### Goals:
// MAGIC The goal of this tutorial is to help you understand about [Delta Lake](https://docs.databricks.com/delta/index.html). Delta Lake is an open source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.
// MAGIC 
// MAGIC Specifically, Delta Lake offers:
// MAGIC 
// MAGIC ***ACID transactions on Spark***: Serializable isolation levels ensure that readers never see inconsistent data.  
// MAGIC 
// MAGIC ***Scalable metadata handling:*** Leverages Spark’s distributed processing power to handle all the metadata for petabyte-scale tables with billions of files at ease.  
// MAGIC 
// MAGIC ***Streaming and batch unification:*** A table in Delta Lake is a batch table as well as a streaming source and sink. Streaming data ingest, batch historic backfill, interactive queries all just work out of the box.  
// MAGIC 
// MAGIC ***Schema enforcement:*** Automatically handles schema variations to prevent insertion of bad records during ingestion.  
// MAGIC 
// MAGIC ***Time travel:*** Data versioning enables rollbacks, full historical audit trails, and reproducible machine learning experiments.  
// MAGIC 
// MAGIC 
// MAGIC Delta Lake on Databricks allows you to configure Delta Lake based on your workload patterns and provides optimized layouts and indexes for fast interactive queries. For information on Delta Lake on Databricks, see [Optimizations](https://docs.databricks.com/delta/delta-on-databricks.html#delta-on-databricks)
// MAGIC 
// MAGIC ![DL1](https://databricks.com/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png)
// MAGIC 
// MAGIC #### Data: We will use the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml). 
// MAGIC  
// MAGIC #### Task: This tutorial covers the following tasks:
// MAGIC - How to create  table in Delta Lake format
// MAGIC - How to perform these operations on a Delta Lake table: insert, update, merge, append, delete
// MAGIC - How to improve performance using OPTIMIZED and ZORDER
// MAGIC - How to use Delta Lake tables with Structured Streaming

// COMMAND ----------

// MAGIC %md 
// MAGIC ### One Prerequisite: 
// MAGIC You will need acceess to the Azure blob storage container that contains the NY Taxi data. Configure access to the storage account in cmd 3

// COMMAND ----------

//SPECIFY ACCCESS DETAILS FOR THE AZURE BLOB STORAGE CONTAINER WITH THE NY TAXI DATA. 

val storageAccountName = "nytaxidata"
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="

val containerName = "taxidata"

spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Tasks
// MAGIC 
// MAGIC ##### Task 1: Basic Delta Lake Operations
// MAGIC  - 1.1 Create a Delta Lake table
// MAGIC  - 1.2 Insert data to the table
// MAGIC  - 1.3 Merge Into table
// MAGIC  - 1.4 Update table
// MAGIC  - 1.5 Delete from table
// MAGIC 
// MAGIC ##### Task 2: Get table metadata 
// MAGIC 
// MAGIC ##### Task 3: Improving performance
// MAGIC 
// MAGIC ##### Task 4: Analyze streaming data with Structured Streaming

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Basic Delta Lake Table Operations
// MAGIC 
// MAGIC #### Task 1.1: Create a Delta Lake table
// MAGIC 
// MAGIC Let us load 5000 records from our NY Taxi data from Azure Blob Storage and load in the Delta Lake table.
// MAGIC 
// MAGIC <b>Note:</b> We can use <b>existing</b> Spark SQL code. We just need to change the formart from parquet (or CSV or JSON) to Delta.

// COMMAND ----------

// DBTITLE 0,Load Sample Data
import org.apache.spark.sql.functions._

spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

//Read 5000 records from Azure Blob Storage

val wasbsPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2018-06.csv"
var tripdata = spark.read.option("header", true).csv(wasbsPath).limit(5000)

//write the data to a file in Delta Lake format
tripdata.write.format("delta").mode("overwrite").save("/data/trip_data")


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Create a Delta Lake table directly from the file. 
// MAGIC -- The default location pf the table is: `/user/hive/warehouse/<table-name>`
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trip_data_delta;
// MAGIC CREATE TABLE trip_data_delta
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/data/trip_data`;
// MAGIC 
// MAGIC -- check that the table has 5000 records
// MAGIC SELECT count(*) from trip_data_delta;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- now show the content of the table
// MAGIC SELECT * FROM trip_data_delta;

// COMMAND ----------

// You can also read the contents of a Delta Lake table without using SQL as follows

val tripDataDelta = spark.table("trip_data_delta")

display(tripDataDelta)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Note: Once a dataframe from Delta Lake source is generated,  you can click on the dataframe name in the notebook state panel to view the metadata related to our delta table. The metadata includes Schema, Details and History. Try this in the next command.
// MAGIC 
// MAGIC 
// MAGIC 1. Schema
// MAGIC 
// MAGIC ![schema](https://nytaxidata.blob.core.windows.net/notebookimages/schema.png)
// MAGIC 
// MAGIC 2. Details
// MAGIC 
// MAGIC ![details](https://nytaxidata.blob.core.windows.net/notebookimages/details.png)
// MAGIC 
// MAGIC 3. History
// MAGIC 
// MAGIC ![history](https://nytaxidata.blob.core.windows.net/notebookimages/history.png)

// COMMAND ----------

tripDataDelta

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Basic Delta Lake Table Operations
// MAGIC 
// MAGIC ### Task 1.2: Insert data to a table ###
// MAGIC 
// MAGIC As new records arrive, you can atomically insert them to the Delta Lakes table.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- inset a single record into the table 
// MAGIC INSERT INTO trip_data_delta VALUES (1,"2018-11-30 18:53:00", "2018-11-30 18:53:00",0,0,0,"NNN",0,0,0,0,0,0,0,0,0,0)
// MAGIC 
// MAGIC -- no need to change the data type while appending the data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Let us check to make sure the count has increased
// MAGIC 
// MAGIC SELECT count(*) FROM trip_data_delta

// COMMAND ----------

// Let us first creare a new record
import org.apache.spark.sql.types._

val newRecord = Seq(("0","2018-11-30 18:53:00",
                     "2018-11-30 18:53:00",
                     "0",
                     "0",
                     "0",
                     "N",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0",
                     "0"))
.toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance","RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount")

display(newRecord)

// COMMAND ----------

// Append this new record to the table.

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("tripDataDelta")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM trip_data_delta

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Basic Delta Lake Table Operations
// MAGIC 
// MAGIC #### Task 1.3:  Using the 'MERGE INTO' operator
// MAGIC 
// MAGIC The MERGE INTO statement allows you to merge a set of updates and insertions into an existing dataset.
// MAGIC - If the record already exist then it is updated. 
// MAGIC - If the record does not exist then it is inserted.
// MAGIC 
// MAGIC We show this as follows:
// MAGIC 
// MAGIC - First we will use MERGE INTO to insert records that <b> already exists </b> in the table. You will notice that the table <b> count will not increase, but the records will be updated </b>.
// MAGIC 
// MAGIC - Next we will use MERGE INTO to insert records <b> do not exist </b> in the table. You will notice that the table <b> count will increase </b> correspondingly.

// COMMAND ----------

// First create a new Delta Lake file with new records by reading the file 'yellow_tripdata_2017-01.csv' stored in Azure Blob Storage. 
// Note that the Delta Lake table created above was based on the file 'yellow_tripdata_2018-06.csv'

val newDataPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-01.csv"

val dataForMerge = spark.read.option("header", true).csv(newDataPath).limit(5000)

dataForMerge.write.format("delta").mode("overwrite").save("/data/trip_data_delta_merge")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --Create a Databricks Delta table from the file '/data/trip_data_delta_merge' created above
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trip_data_delta_merge;
// MAGIC CREATE TABLE trip_data_delta_merge
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/data/trip_data_delta_merge`;
// MAGIC 
// MAGIC -- Check that the table has 5000 records.
// MAGIC 
// MAGIC SELECT count(*) FROM trip_data_delta_merge;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Now MERGE the second table (trip_data_delta_merge) with the first one (trip_data_delta)
// MAGIC 
// MAGIC MERGE INTO trip_data_delta
// MAGIC USING trip_data_delta_merge
// MAGIC ON trip_data_delta.VendorID = trip_data_delta_merge.VendorID AND trip_data_delta.tpep_pickup_datetime = trip_data_delta_merge.tpep_pickup_datetime AND trip_data_delta.tpep_dropoff_datetime = trip_data_delta_merge.tpep_dropoff_datetime AND trip_data_delta.trip_distance = trip_data_delta_merge.trip_distance
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC      trip_data_delta.passenger_count = trip_data_delta_merge.passenger_count 
// MAGIC     --,trip_data_delta.trip_distance = trip_data_delta_merge.trip_distance
// MAGIC     ,trip_data_delta.RatecodeID = trip_data_delta_merge.RatecodeID
// MAGIC     ,trip_data_delta.store_and_fwd_flag = trip_data_delta_merge.store_and_fwd_flag
// MAGIC     ,trip_data_delta.PULocationID = trip_data_delta_merge.PULocationID
// MAGIC     ,trip_data_delta.DOLocationID = trip_data_delta_merge.DOLocationID
// MAGIC     ,trip_data_delta.payment_type = trip_data_delta_merge.payment_type
// MAGIC     ,trip_data_delta.fare_amount = trip_data_delta_merge.fare_amount
// MAGIC     ,trip_data_delta.extra = trip_data_delta_merge.extra
// MAGIC     ,trip_data_delta.mta_tax = trip_data_delta_merge.mta_tax
// MAGIC     ,trip_data_delta.tip_amount = trip_data_delta_merge.tip_amount
// MAGIC     ,trip_data_delta.tolls_amount = trip_data_delta_merge.tolls_amount
// MAGIC     ,trip_data_delta.improvement_surcharge = trip_data_delta_merge.improvement_surcharge
// MAGIC     ,trip_data_delta.total_amount = trip_data_delta_merge.total_amount
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT (trip_data_delta.VendorID, trip_data_delta.tpep_pickup_datetime, trip_data_delta.tpep_dropoff_datetime, trip_data_delta.passenger_count, trip_data_delta.trip_distance, trip_data_delta.RatecodeID, trip_data_delta.store_and_fwd_flag, trip_data_delta.PULocationID, trip_data_delta.DOLocationID, trip_data_delta.payment_type, trip_data_delta.fare_amount, trip_data_delta.extra, trip_data_delta.mta_tax, trip_data_delta.tip_amount, trip_data_delta.tolls_amount, trip_data_delta.improvement_surcharge, trip_data_delta.total_amount)
// MAGIC   VALUES (trip_data_delta_merge.VendorID, trip_data_delta_merge.tpep_pickup_datetime, trip_data_delta_merge.tpep_dropoff_datetime, trip_data_delta_merge.passenger_count, trip_data_delta_merge.trip_distance, trip_data_delta_merge.RatecodeID, trip_data_delta_merge.store_and_fwd_flag, trip_data_delta_merge.PULocationID, trip_data_delta_merge.DOLocationID, trip_data_delta_merge.payment_type, trip_data_delta_merge.fare_amount, trip_data_delta_merge.extra, trip_data_delta_merge.mta_tax, trip_data_delta_merge.tip_amount, trip_data_delta_merge.tolls_amount, trip_data_delta_merge.improvement_surcharge, trip_data_delta_merge.total_amount);
// MAGIC   
// MAGIC   -- Now count the number of records. The table should have 5000 additional record
// MAGIC   
// MAGIC   SELECT count(*) FROM trip_data_delta
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Basic Delta Lake Table Operations
// MAGIC 
// MAGIC ####Task 1.4: Update a table using UPDATE
// MAGIC 
// MAGIC Updates the column values for the rows that match a predicate. When no predicate is provided, update the column values for all rows.
// MAGIC In the example below we will change value of the column store_and_fwd_flag from 'N' to 'Y'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- first count how many records have store_and_fwd_flag = 'N'
// MAGIC 
// MAGIC select count(*) 
// MAGIC from trip_data_delta 
// MAGIC WHERE store_and_fwd_flag = 'N';

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC -- first count how many records have store_and_fwd_flag = 'Y'
// MAGIC 
// MAGIC select count(*) 
// MAGIC from trip_data_delta 
// MAGIC WHERE store_and_fwd_flag = 'Y';

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Now update values of store_and_fwd_flag column to 'y' using UPDATE
// MAGIC 
// MAGIC UPDATE trip_data_delta SET store_and_fwd_flag = 'Y' WHERE store_and_fwd_flag = 'N';
// MAGIC 
// MAGIC -- Now do the same count again. This time the result should be zero.
// MAGIC 
// MAGIC select count(*) 
// MAGIC from trip_data_delta 
// MAGIC WHERE store_and_fwd_flag = 'N';

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- To be sure, count how many records have store_and_fwd_flag = 'Y'
// MAGIC 
// MAGIC select count(*) 
// MAGIC from trip_data_delta 
// MAGIC WHERE store_and_fwd_flag = 'Y';

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Basic Delta Lake Table Operations
// MAGIC 
// MAGIC #### Task 1.5: Delete rows from a table using DELETE FROM
// MAGIC As Delta Lake tables maintain a history of table versions, it supports deletion of records which cannot be done in normal SparkSQL Tables.
// MAGIC 
// MAGIC Delete FROM deletes the rows that match a predicate. When no predicate is provided, delete all rows.
// MAGIC 
// MAGIC The following cmd deletes all rows where the column 'passenger_count' has value '1

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- first count how many rows have column 'passenger_count' = 1
// MAGIC 
// MAGIC select count(*) from trip_data_delta where passenger_count = 1;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Now delete all rows with passenger_count = 1
// MAGIC 
// MAGIC DELETE FROM trip_data_delta WHERE passenger_count = 1;
// MAGIC 
// MAGIC -- Count again. The result should be zero.
// MAGIC 
// MAGIC select count(*) from trip_data_delta where passenger_count = 1;

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Task 2: Get Delta Lake table metadata
// MAGIC 
// MAGIC The metadata associated with a table lets you see the table's history, the table's format and other table details.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show history
// MAGIC DESCRIBE HISTORY trip_data_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show table details
// MAGIC DESCRIBE DETAIL trip_data_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show table format
// MAGIC DESCRIBE  FORMATTED trip_data_delta

// COMMAND ----------

// MAGIC %md
// MAGIC ### Task 3: Optimizing performance
// MAGIC 
// MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. Delta Lake supports two layout algorithms:
// MAGIC - bin-packing and 
// MAGIC - Z-Ordering.
// MAGIC 
// MAGIC ##### Compaction (bin-packing)
// MAGIC 
// MAGIC Delta Lake on Databricks can improve the speed of read queries from a table by coalescing small files into larger ones. You trigger compaction by running the <b> OPTIMIZE </b>command.
// MAGIC Note: 
// MAGIC - Bin-packing optimization is idempotent, meaning that if it is run twice on the same dataset, the second instance has no effect.
// MAGIC - Bin-packing aims to produce evenly-balanced data files with respect to their size on disk, but not necessarily number of tuples per file. However, the two measures are most often correlated.
// MAGIC 
// MAGIC Readers of Delta Lake tables use snapshot isolation, which means that they are not interrupted when OPTIMIZE removes unnecessary files from the transaction log. OPTIMIZE makes no data related changes to the table, so a read before and after an OPTIMIZE has the same results. Performing OPTIMIZE on a table that is a streaming source does not affect any current or future streams that treat this table as a source.
// MAGIC 
// MAGIC ##### Z-Ordering (multi-dimensional clustering)
// MAGIC Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms to dramatically reduce the amount of data that needs to be read. To Z-Order data, you specify the columns to order on in the ZORDER BY clause.
// MAGIC 
// MAGIC You can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness of the locality drops with each additional column. Z-Ordering on columns that do not have statistics collected on them would be ineffective and a waste of resources as data skipping requires column-local stats such as min, max, and count

// COMMAND ----------

// MAGIC %md
// MAGIC #### Comparing the performance advantage of using the Delta Lake format vs Parquet format <br> <br>
// MAGIC - We will compare the performance by executing the <b> same query </b>  on data in Parquet format vs the same data in Delta Lake format
// MAGIC - The common query is: <b> "find the top 10 pickup locations, based on the total fare earned by the taxis for each pickup location" </b>
// MAGIC 
// MAGIC Note: We will be building few tables with a 10s of million rows. Some of the operations may take a few minutes depending on your cluster configuration.

// COMMAND ----------

// First, let us look at performance using Parquet format

val path = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-01.csv"

//Read the data from Azure Blob Storage
val cabs = spark.read.option("header", true).csv(path)

// Store the data in Parquet format
cabs.write.format("parquet").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_parquet")


// COMMAND ----------

// Execute the query - NOTE the time taken

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val cabs_parquet = spark.read.format("parquet").load("/tmp/cabs_parquet")

display(cabs_parquet.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// Now store the same data in Delta Lake

cabs.write.format("delta").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_delta")

// Optimize the table

display(spark.sql("DROP TABLE  IF EXISTS cabs"))

display(spark.sql("CREATE TABLE cabs USING DELTA LOCATION '/tmp/cabs_delta'"))
        
display(spark.sql("OPTIMIZE cabs ZORDER BY (payment_type)"))

// COMMAND ----------

// Run the same query - NOTE the time taken. You will notice that this query takes substantially less time than the previous one.

val cabs_delta = spark.read.format("delta").load("/tmp/cabs_delta")

display(cabs_delta.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Garbage Collection using VACCUM
// MAGIC 
// MAGIC To ensure that concurrent readers can continue reading a stale snapshot of a table, Delta Lake on Databricks leaves deleted files on DBFS for a period of time. To save on storage costs you should occasionally clean up these invalid files using the VACUUM command. You can also specify DRY RUN to test the vacuum and return a list of files to be deleted

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM trip_data_delta DRY RUN

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Task 4: DATABRICKS DELTA WITH STRUCTURED STREAMING##
// MAGIC Databricks Delta is deeply integrated with Spark Structured Streaming through readStream and writeStream. Databricks Delta overcomes many of the limitations typically associated with streaming systems and files, including:
// MAGIC 
// MAGIC    - Coalescing small files produced by low latency ingest.
// MAGIC    - Maintaining “exactly-once” processing with more than one stream (or concurrent batch jobs).
// MAGIC    - Efficiently discovering which files are new when using files as the source for a stream.
// MAGIC 
// MAGIC #### Delta Lake table as a stream source
// MAGIC 
// MAGIC When you load a Delta Lake table as a stream source and use it in a streaming query, the query processes all of the data present in the table as well as any new data that arrives after the stream is started.
// MAGIC 
// MAGIC #### Delta Lake table as a sink
// MAGIC You can also write data into a Delta Lake table using Structured Streaming. The transaction log enables Delta Lake to guarantee exactly-once processing, even when there are other streams or batch queries running concurrently against the table.
// MAGIC 
// MAGIC By default, streams run in append mode, which adds new records to the table
// MAGIC 
// MAGIC #### Ignoring Updateds and deletes
// MAGIC 
// MAGIC Structured Streaming does not handle input that is not an append and throws an exception if any modifications occur on the table being used as a source. There are two main strategies for dealing with changes that cannot be propagated downstream automatically:
// MAGIC 
// MAGIC - Since Databricks Delta tables retain all history by default, in many cases you can delete the output and checkpoint and restart the stream from the beginning.
// MAGIC - You can set either of these two options:
// MAGIC  - ignoreDeletes skips any transaction that deletes entire partitions from the source table. For example, you can set this option if you delete data from the source table that was previously kept for data retention purposes and you do not want these deletions to propagate downstream.
// MAGIC  - ignoreChanges skips any transaction that updates data in the source table. For example, you can set this option if you use UPDATE, MERGE, DELETE, or OVERWRITE and these changes do not need to be propagated downstream
// MAGIC  
// MAGIC ### Command Flow: We will execute the following sequence of commands 
// MAGIC 
// MAGIC  - Create two Delta Lake tables (identical schema) -- one as a 'stream source' ('trip_data_delta_streamsource')and one as a 'stream sink' (trip_data_delta_streamsink)
// MAGIC  - Create an event stream that:
// MAGIC   - uses 'trip_data_delta_streamsource' as <b>stream source </b>. Uses the 'readStream()' method.
// MAGIC   - uses 'trip_data_delta_streamsink') as a <b> stream sink </b>. Uses the 'writeStream()'
// MAGIC  - Populate the 'source table'. All the events should eventually end up in the 'sink table'.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Create the source table
// MAGIC 
// MAGIC Drop Table if exists trip_data_delta_streamsource;
// MAGIC CREATE TABLE trip_data_delta_streamsource (
// MAGIC   VendorID STRING
// MAGIC  ,tpep_pickup_datetime STRING
// MAGIC  ,tpep_dropoff_datetime STRING
// MAGIC  ,passenger_count STRING
// MAGIC  ,trip_distance STRING
// MAGIC  ,RatecodeID STRING
// MAGIC  ,store_and_fwd_flag STRING
// MAGIC  ,PULocationID STRING
// MAGIC  ,DOLocationID STRING
// MAGIC  ,payment_type STRING
// MAGIC  ,fare_amount STRING
// MAGIC  ,extra STRING
// MAGIC  ,mta_tax STRING
// MAGIC  ,tip_amount STRING
// MAGIC  ,tolls_amount STRING
// MAGIC  ,improvement_surcharge STRING
// MAGIC  ,total_amount STRING)
// MAGIC USING DELTA;
// MAGIC 
// MAGIC -- Create the sink table
// MAGIC 
// MAGIC Drop Table if exists trip_data_delta_streamsink;
// MAGIC CREATE TABLE trip_data_delta_streamsink (
// MAGIC   VendorID STRING
// MAGIC  ,tpep_pickup_datetime STRING
// MAGIC  ,tpep_dropoff_datetime STRING
// MAGIC  ,passenger_count STRING
// MAGIC  ,trip_distance STRING
// MAGIC  ,RatecodeID STRING
// MAGIC  ,store_and_fwd_flag STRING
// MAGIC  ,PULocationID STRING
// MAGIC  ,DOLocationID STRING
// MAGIC  ,payment_type STRING
// MAGIC  ,fare_amount STRING
// MAGIC  ,extra STRING
// MAGIC  ,mta_tax STRING
// MAGIC  ,tip_amount STRING
// MAGIC  ,tolls_amount STRING
// MAGIC  ,improvement_surcharge STRING
// MAGIC  ,total_amount STRING)
// MAGIC USING DELTA

// COMMAND ----------


dbutils.fs.rm("/data/_checkpoint",true)

// Create a stream that uses the stream source table created above
val events = spark.readStream.table("trip_data_delta_streamsource")

// And uses the second table created above as sink (using the options ignoreDeletes and ignoreChanges to handle updates and deletes)
events.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/data/_checkpoint")
  .option("ignoreDeletes", true)
  .option("ignoreChanges", true)
  .table("trip_data_delta_streamsink")

// now populate the stream source table

// First read the data from Azure Blob Storage

val newDataPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-02.csv"

val dataForStream = spark.read.option("header", true).csv(newDataPath).limit(5000)

//insert the data into the stream source table.
dataForStream.write.format("delta").mode("append").saveAsTable("trip_data_delta_streamsource")


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Check that the source stream table is indeed properly populated
// MAGIC 
// MAGIC select count(*) from trip_data_delta_streamsource

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- Finally check if the events have made it from the source table to the sink table. The count in stream source and stream tables should be the same
// MAGIC SELECT count(*) FROM trip_data_delta_streamsink

// COMMAND ----------

// MAGIC %md
// MAGIC ## The following needs a better explantion

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE trip_data_delta

// COMMAND ----------

// MAGIC %md
// MAGIC #####ZORDER#####
// MAGIC ZOrdering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Databricks Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read. To ZOrder data, you specify the columns to order on in the ZORDER BY clause:

// COMMAND ----------

// MAGIC %md
// MAGIC OPTIMIZE consolidates files and orders the Databricks Delta table `trip_data_delta` data by `tpep_pickup_datetime` under each partition for faster retrieval

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE trip_data_delta ZORDER BY (tpep_pickup_datetime);
