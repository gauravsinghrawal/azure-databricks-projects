// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # About this Tutorial #  
// MAGIC #### Goals: The of this tutorial us to help you understand how to use Azure Spark Structured Streaming for analysis of streaming data. 
// MAGIC Structured Streaming is the Apache Spark API that lets you express computation on streaming data in the same way you express a batch computation on static data. The Spark SQL engine performs the computation incrementally and continuously updates the result as streaming data arrives.
// MAGIC 
// MAGIC Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write Ahead Logs. In short, Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.
// MAGIC  
// MAGIC  #### Data: The tutorial uses the publicly available [NYC Taxi Trip Record](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml).
// MAGIC  
// MAGIC  #### Learnings: In this tutorial you will learn the following:<br>
// MAGIC  - How to send and read data to and from <b>Azure EventHubs</b>.
// MAGIC  - How to process data in Azure EventHubs with Spark Structred  Streaiming.
// MAGIC  - How to perform the following operations on the streaming data using Spark Structured Streamimhg
// MAGIC    - Realtime analystics.
// MAGIC    - Aggregations and filtering.
// MAGIC    - Windowed operations based on event time.
// MAGIC    - Join Operations between static and streaming data
// MAGIC  - How to do predictions on streaming data using a machine learning model.

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ## Integration with Azure Event Hubs ## 
// MAGIC 
// MAGIC This tutorial uses Azure Event Hubs as the source of the streaming data for Spark Streaming. Azure Event Hubs is a Big Data streaming platform and event ingestion service, capable of receiving and processing millions of events per second. 
// MAGIC - Azure Event Hubs can process and store events, data, or telemetry produced by distributed software and devices. 
// MAGIC - Data sent to an event hub can be transformed and stored using any real-time analytics provider or batching/storage adapters. 
// MAGIC - Azure Event Hubs represents the "front door" for an event pipeline, often called an "event ingestor". An <i>event ingestor</i> is a component or service that sits between event publishers and event consumers to decouple the production of an event stream from the consumption of those events. 
// MAGIC - Event Hubs provides a unified streaming platform with time retention buffer, decoupling the event producers from event consumers.
// MAGIC 
// MAGIC <b>Azure Event Hubs Spark Connector</b>, developed by Microsoft, integrates Azure Event Hubs with Spark Structured Streaming. Spark Structured Streaming can read and analyze data from Azure Events Hubs. 
// MAGIC 
// MAGIC ![Structured Streaming](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-eventhubs-tutorial.png)
// MAGIC 
// MAGIC For estabilishing connectivity between Azure Event Hubs and Databricks, refer to the Azure Databricks [documentation](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html).

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ##3 (Three) Prequesites:
// MAGIC You will need access to the following three resources to proceed with the tutorial: <br><br>
// MAGIC 
// MAGIC 1. An <b>Azure Blob Storage account</b> that contains the NY Taxi Data. Configure the access to the account in *cmd 4* below.
// MAGIC  - In addition to the NY Taxi data, the Azure storage account should also have a <b> Lookup </b> table. <br><br>
// MAGIC 2. An <b> Event Hubs mamespace </b> and an <b> event hub </b>. Configure the access in <b>cmd 5</b> below.
// MAGIC   - Note: If the Event Hubs NameSpace and Event Hub have not already been provisioned for you, or if you want your own instance, you can create them using the Azure portal using these [instructions](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create#create-an-event-hubs-namespace). <br><br>
// MAGIC 3. A <b> Pre-trained Spark MLlib Model: </b> The Spark MLlib model is used to do predictions on the streaming data. The model was created in the previous lab and stored persistently. In this tutorial we will read the stored model and activate it. Specify the location of the stored model in <b> cmd 6 </b> below.

// COMMAND ----------

//Configure access to the storage account here.
val storageAccountName = "nytaxidata"
val storageAccountAccessKey = "u0IA8jQWU5TwGwL61tb/onFAOHswohn8eJdy0UECLwXIXGP5FqivMS6yvUnzXigs489oj7q2z35deva+QFbjtw=="

val containerName = "taxidata"
val wasbsPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-*.csv"
val lookUpPath = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/taxi _zone_lookup.csv"
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)

// COMMAND ----------

//Configure access to the Azure EventHubs namespace and event hub here
//Set up Connection to Azure Event Hubs
// Name of the Event Hubs space
val namespaceName = "nycTaxiStream" 
//Name of the event hub within the namespac
val eventHubName = "streaming_data_hub"
// Name of the shared access policy - You have to create this explicitly after creating the event hub 
val sasKeyName = "simplepolicy"  
// Primary key for the above policy - This can be obtained from the Portal. It is generted for you.
val sasKey = "2JI55yQIuGzFWpk9z0wB4ZnH5YRjo8R7xW8LqPGnnGg="

// COMMAND ----------

//Specify the storage path where the Spark MLlib is stored
val modelpath = "/FileStore/ml/models/"

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Setup: Follow these instructions before running the notebook. ###
// MAGIC 
// MAGIC 1. <b> Create a cluster: </b> Make sure to create a cluster with Databricks <b>version 5.5</b>. Lower versions will not work with some cmds, specifically cmd 13.
// MAGIC 
// MAGIC 2. <b> Install Library </b>: Install the <b> The Azure Event Hubs Spark Connector library </b> so that the Azure Events Hubs API can be accessed from this notebook. <br><br>
// MAGIC 
// MAGIC   - Create a library in your Databricks workspace using the Maven coordinates <b> `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.3` </b>
// MAGIC   - Attach the created library to your cluster.  
// MAGIC 
// MAGIC   - The instructions for installing a library can be found here: https://docs.databricks.com/user-guide/libraries.html#maven-libraries

// COMMAND ----------

// MAGIC %md ## Tutorial Flow: The tutorial has the following flow.<br>
// MAGIC 
// MAGIC 1. Step 1: Create a streaming data source by reading data from Azure Blob Storage and sending to Azure Event Hubs.
// MAGIC 
// MAGIC 2. Step 2: Read data from Event Hubs using the `readStream` method of Structured Streaming API. This generates a stream of data that can be processed by Spark Streaming APIs.
// MAGIC 
// MAGIC 3. Step 3: Analyze the streaming data using Structured Streaming APIs. The following queries are implemented.
// MAGIC  
// MAGIC  - Query 3.1 Find the moving count of the total number passengers in New York City for each area
// MAGIC  
// MAGIC  - Query 3.2 Get real time average earning of all cab vendors
// MAGIC  
// MAGIC  - Query 3.3 Find the moving count of payments made by each payment mode.
// MAGIC  
// MAGIC  - Query 3.4 Cumulative sum of Distance covered and Fare earned by all Cabs of both the Vendors
// MAGIC  
// MAGIC  - Query 3.5 Find the busiest route by getting the highest real time passenger count between all pickups and dropoff locations
// MAGIC  
// MAGIC  - Query 3.6 Get top pickup locations with most passengers in last 10 seconds.
// MAGIC  
// MAGIC  - Query 3.7 Use a lookup table to get the Borough, Zone and 'Service Zone' for each incoming event.
// MAGIC  
// MAGIC 4. Perform predictions on streaming data using the pre-trained ML model. This is done with the following four steps:
// MAGIC  
// MAGIC  - 4.1 Transform the streaming data so that it conforms to the schema expected by the model.
// MAGIC  
// MAGIC  - 4.2 Read transformed data as real time stream
// MAGIC 
// MAGIC  - 4.3 Read the ML model from persistent storage
// MAGIC  
// MAGIC  - 4.4 Do predictions on the real time stream 
// MAGIC 
// MAGIC   

// COMMAND ----------

// MAGIC %md
// MAGIC ### Points to note: Read before doing the lab
// MAGIC 
// MAGIC  1. We read data from Azure Blob Storage (the source) and send to Azure Events. However, we are sending only 10,000 records to Event Hubs, even though the source has many more records.This is because the 'collect operation' to get the data into a list is a compute and memory intensive opartion. We want this operation to run in a reasonable amount of time. You can remove the limit by modifying the code in *cmd 12* 
// MAGIC  
// MAGIC  2. Due to the above limit, when all 10,000 events have been generated, the stream will be 'empty'.  As a result some Spark Streamimg queries will not get any input events. In this case, <b> simply rerun Step 2 </b> to generate a fresh stream.
// MAGIC  
// MAGIC  3. The Spark Streaming queries in Step 3.1 through 3.7 do not terminate on thier own. So after successfully executing each query, you can manually stop the execution by clicking on the `cancel` link. 
// MAGIC  
// MAGIC  4. We will <b>not</b> be performing the Spark Streaming operations <b>directly </b> on data from Azure Event Hubs. Rather, we will store the streaming data in a staging location in DBFS. This does not change the way the Streaming operations are implemented. It protects us from failures of Spark jobs if the incoming stream from Azure Event Hubs is interrupted for any reason.
// MAGIC 
// MAGIC  5. Azure Event Hubs has a small internal buffer (~256kB by default). So, the incoming stream for successive queries might be slow or take some time to load. Please wait for a couple of minutes after firing your query to see the results appear. You can track the progress of incoming events in the "Input vs Processing Rate graph" by clicking on <b>'display_query'<b/>
// MAGIC   
// MAGIC  6. You can see the status of the stream in the 'command window' as shown below.
// MAGIC 
// MAGIC   ![Stream Status](https://docs.databricks.com/_images/gsasg-stream-status.png)
// MAGIC 
// MAGIC  7. You can expand `display_query` to get a dashboard showing the number of records processed, batch statistics, and the state of the aggregation as shown below.
// MAGIC 
// MAGIC   ![Structured Streaming Dashboard](https://docs.databricks.com/_images/gsasg-streaming-dashboard.png)

// COMMAND ----------

// DBTITLE 1,Remove 'tmp' files.
// MAGIC %python
// MAGIC data = dbutils.fs.ls("/tmp")
// MAGIC filelist = []
// MAGIC for file in data: 
// MAGIC   try:
// MAGIC     dataFile = file[0]
// MAGIC     if(dbutils.fs.rm(dataFile,True))==True:
// MAGIC       data.remove(file)
// MAGIC   except:
// MAGIC     data.remove(file)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Step 1: Read data from Azure Blob Storage and write to Azure Event Hubs ###
// MAGIC 
// MAGIC We will read the NYC Taxi Data from the Azure Blob Storage and convert it into a real time stream by sending the batch data to Azure Event Hubs. Azure Event Hubs will create seperate events from the batch data, allowing us to read it in realtime via the Event Hubs Receiver.
// MAGIC 
// MAGIC We will read our the CSV file stored in Azure Blob Storage and collect it in a list using Spark's RDD API. Then we will send each element of this list to  Event Hubs sender that we will create.
// MAGIC 
// MAGIC The Dataframe being written to EventHubs should have the following columns in the schema:
// MAGIC 
// MAGIC **Column**               **Type** 
// MAGIC 1. body                      *binary*
// MAGIC 2. partition 	             *string*
// MAGIC 3. offset                   *string*
// MAGIC 4. sequenceNumber           *Long*
// MAGIC 5. enqueuedTime             *timestamp*
// MAGIC 6. publisher                *string*
// MAGIC 7. partitionKey             *string*
// MAGIC 8. properties 	             *map[string,json]*
// MAGIC 
// MAGIC The body is always provided as a byte array. Use cast("string") to explicitly deserialize the body column.
// MAGIC 
// MAGIC The body column is the only required option. If a partitionId and partitionKey are not provided, then events will distributed to partitions using a round-robin model.
// MAGIC Users can also provided properties via a map[string,json] if they would like to send any additional properties with their events.

// COMMAND ----------

import java.util._
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.EventHubsConf
import java.util.concurrent._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//Read the data from the csv files
var tripdata = spark.read.option("header", true).csv(wasbsPath).limit(10000).rdd.collect.toList

//Set up Connection to Azure Event Hubs
val connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.create(connStr.toString(), pool)

def sendEvent(message: String) = {
    val messageData = EventData.create(message.getBytes("UTF-8"))
        eventHubClient.get().send(messageData)
    };

val customEventhubParameters =  EventHubsConf(connStr.toString()).setMaxEventsPerTrigger(1) //set the maximun event at a time



// We will create a function to send records to Event Hubs. This is done to simulate a realtime stream of events. Whenever we use our incoming stream, we can call sendingEvents() function to send fresh events so that our analysis is performed on a realtime stream.

def sendingData(count : Int) : Unit ={
  // Send stream to Event Hubs
  for( a <- 0 to (tripdata.length - 1)){
          sendEvent(tripdata(a).toString)
      }
}


// COMMAND ----------

// MAGIC %md 
// MAGIC ### Step 2: Read incoming data stream from Event Hubs using readStream() ###
// MAGIC 
// MAGIC We will read the incoming stream using the `readStream` method of the Structured Streaming API. This will generate a stream of data that can subsequently be used by the Spark Streaming functions. 
// MAGIC 
// MAGIC <b> Note: </b>This cmd will not terminate. 
// MAGIC - It runs continously, reading events from Azure Event Hubs. 
// MAGIC - However, after 10,000 events there will be no more events to read. 
// MAGIC - Rerun the cmd to generate a fresh set of 10,000 events.

// COMMAND ----------

//NOTE: In case the stream dies during the course of this lab due to any failure of Event Hubs or Spark Stream Reader, you can rerun this command in order to initiliase the stream again

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
 
// Sending the incoming stream into the console.
// Data comes in batches!
// Event Hub message format is JSON and contains "body" field


// Body is binary, so we cast it to string to see the actual content of the message
val messages = incomingStream.withColumn("Body", $"body".cast(StringType))

sendingData(1)
display(messages)


// COMMAND ----------

// The streaming data has the following information: Offset, Timestamp, readable Time and the Body. We will extract the 'Body' from the stream as that contains our data.

// Also, in order to avoid failure of Spark Jobs if the incoming stream from Event Hubs is interrupted, we will write our stream to a Staging location in DBFS and read it from the Staging location for further analytics.

val messages1 = messages.select("Body") //select the body column which contains the data.
var cleanUDF = udf((s:String)=> {s.replaceAll("[\\[\\]]","")})

//Parse the data using ',' separator from the Body.  
val streamData = (
    messages1
   .withColumn("_tmp", split(cleanUDF($"Body"), ","))
   .select($"_tmp".getItem(0).as("VendorID")
          ,$"_tmp".getItem(1).cast("timestamp").as("tpep_pickup_datetime")
          ,$"_tmp".getItem(2).cast("timestamp").as("tpep_dropoff_datetime")
          ,$"_tmp".getItem(3).cast("int").as("passenger_count")
          ,$"_tmp".getItem(4).cast("float").as("trip_distance")
          ,$"_tmp".getItem(5).as("RatecodeID")
          ,$"_tmp".getItem(6).as("store_and_fwd_flag")
          ,$"_tmp".getItem(7).as("PULocationID")
          ,$"_tmp".getItem(8).as("DOLocationID")
          ,$"_tmp".getItem(9).as("payment_type")
          ,$"_tmp".getItem(10).cast("float").as("fare_amount")
          ,$"_tmp".getItem(11).cast("float").as("extra")
          ,$"_tmp".getItem(12).cast("float").as("mta_tax")
          ,$"_tmp".getItem(13).cast("float").as("tip_amount")
          ,$"_tmp".getItem(14).cast("float").as("tolls_amount")
          ,$"_tmp".getItem(15).cast("float").as("improvement_surcharge")
          ,$"_tmp".getItem(16).cast("float").as("total_amount")
         )
   .drop("_tmp")
)
sendingData(2)
display(streamData)

// COMMAND ----------

// MAGIC %md ### Step 3: STRUCTURED STREAMING API ##
// MAGIC 
// MAGIC Structured Streaming is integrated into Spark’s Dataset and DataFrame APIs; in most cases, you only need to add a few method calls to run a streaming computation. It also adds new operators for windowed aggregation and for setting parameters of the execution model (e.g. output modes). 
// MAGIC 
// MAGIC ##### API Basics:
// MAGIC Streams in Structured Streaming are represented as DataFrames or Datasets with the isStreaming property set to true. You can create them using special read methods from various sources.
// MAGIC 
// MAGIC 
// MAGIC ##### Mapping, Filtering and Running Aggregations
// MAGIC Structured Streaming programs can use DataFrame and Dataset’s existing methods to transform data, including map, filter, select, and others. In addition, running (or infinite) aggregations, such as a count from the beginning of time, are available through the existing APIs.

// COMMAND ----------

// MAGIC %md  ### Query 3.1: Find moving count of total passengers in New York City for each area###
// MAGIC 
// MAGIC The count increases as the passengers arrrive in a particular. 
// MAGIC - Use `groupBy` on Pickup 'LocationID', to get the the total passengers in every unique area.
// MAGIC - USe `orderBy` on sum of passengers to get areas with the most number of passangers, in descending order 

// COMMAND ----------

val movingCountbyLocation = (
    streamData
   .groupBy($"PULocationID")
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"passenger_count".desc)
)
sendingData(3)
display(movingCountbyLocation)

// COMMAND ----------

// MAGIC %md ### Query 3.2: Get real time average earnings of all cab vendors###
// MAGIC 
// MAGIC - Use `groupBy` on VendorID to get unique vendor
// MAGIC - Use `aggregate` `average` on total_amount to get the average amount earned by the vendors.

// COMMAND ----------

val realtimeAverageFarebyVendor = (
    streamData
   .groupBy($"VendorID")
   .agg(avg($"total_amount").as("avg_amount"))
   .orderBy($"avg_amount".desc)
)
sendingData(4)
display(realtimeAverageFarebyVendor)

// COMMAND ----------

// MAGIC %md ### Query 3.3: Find the moving count of payments made by each payment mode.###
// MAGIC 
// MAGIC Using `groupBy` on payment_type to get unique payment_type and aggregate sum on amount with count of payment type to get the no. of payments made by each payment_type with the total amount which get paid through the particular payment type.

// COMMAND ----------

val movingCountByPayment = (
    streamData
   .groupBy($"payment_type")
   .agg(sum($"total_amount"),count($"payment_type"))
)
sendingData(5)
display(movingCountByPayment)

// COMMAND ----------

// MAGIC %md ### Query 3.4: Cumulative sum of distance covered, and fare earned, by all cabs, of both the vendors###
// MAGIC 
// MAGIC Uses groupBy on VendorID and date to get the unique vendorID with the each date while aggregate sum of passenger_count, trip_distance and total_amount and ordered by the date to get the highest gained vendor on a particular date, Considering the total passanger, trip distance and total amount on that particular date. 

// COMMAND ----------

val sumDistanceAndFareByVendor = (    
    streamData
   .groupBy($"VendorID",to_date($"tpep_pickup_datetime").alias("date"))
   .agg(sum($"passenger_count").as("total_passenger"),sum($"trip_distance").as("total_distance"),sum($"total_amount").as("total_amount"))
   .orderBy($"date")
  )
sendingData(6)
display(sumDistanceAndFareByVendor)

// COMMAND ----------

// MAGIC %md ### Query 3.5: Find the busiest route based on highest real time passenger count between all pickup and dropoff locations###
// MAGIC 
// MAGIC Grouped the pickup location, Drop location and date to get the unique route of between two location with aggregating count of total passanger on the route while ordering the date and passanger count in desending order to get the latest date and most no of passanger on that particular route. 

// COMMAND ----------

val busiestRoutesStream = (
    streamData
   .groupBy($"PULocationID", $"DOLocationID",to_date($"tpep_pickup_datetime").alias("date"))
   .agg(count($"passenger_count").as("passenger_count"))
   //.orderBy($"date".desc,$"passenger_count".desc)
)
sendingData(7)
display(busiestRoutesStream)                                                               

// COMMAND ----------

// MAGIC %md ### Query 3.6:  Get top pickup locations with most passengers in last 10 seconds.###
// MAGIC 
// MAGIC #### Windowed Aggregations on Event Time
// MAGIC Streaming applications often need to compute data on various types of windows, including sliding windows, which overlap with each other (e.g. a 1-hour window that advances every 5 minutes), and tumbling windows, which do not (e.g. just every hour). In Structured Streaming, windowing is simply represented as a <b> group-by </b>. Each input event can be mapped to one or more windows, and simply results in updating one or more result table rows.
// MAGIC 
// MAGIC #####  Window functions :
// MAGIC Window aggregate functions are functions that perform a calculation over a group of records called <b> window </b> that are in <i> some </i>relation to the current record (i.e. can be in the same partition or frame as the current row).In other words, when executed, a window function computes a value for each and every row in a window.
// MAGIC 
// MAGIC Query Implementation:
// MAGIC - Use groupBy on Pickup LocationID and (window function) on pickup datetime to get the time window of 10 seconds according to the location
// MAGIC - Use aggregate on sum of passenger in that particular window of time with location, 
// MAGIC - Use order by  the window and passanger count to get the top pickup location with most passenger in last 10 seconds of time duration. 

// COMMAND ----------

val topPickupLocationsByWindow = (
    streamData
   .groupBy($"PULocationID",window($"tpep_pickup_datetime", "10 second", "10 second"))
   .agg(sum($"passenger_count").as("passenger_count"))
   .orderBy($"window".desc,$"passenger_count".desc)
   .select("PULocationID","window.start","window.end","passenger_count")
)
sendingData(8)
display(topPickupLocationsByWindow)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Query 3.7: Find Borough, Zone and Service Zone for each incoming event
// MAGIC 
// MAGIC #### Joining Streams with Static Data ####
// MAGIC 
// MAGIC The Borough, Zone and Service Zone information is not part of the streamimg data; it exists in a seperate, external <b> Lookup </b> table. So, we will perform a <b> join </b> between the streaming data and the lookup data to find the Borough, Zone and Service Zone for each event. The 'join' will be done using the 'Location ID' field, which exists in both the streamind data and the Lookup table. 
// MAGIC 
// MAGIC Because Structured Streaming simply uses the DataFrame API, it is straightforward to join a stream against a static DataFrame. Moreover, the static DataFrame could itself be computed using a Spark query, allowing us to mix batch and streaming computations.

// COMMAND ----------

//LOAD AND EXAMINE THE LOOKUP TABLE
val lookUp = spark.read.option("header", true).csv(lookUpPath)
display(lookUp)

// COMMAND ----------

sendingData(8)
val joinData = (
    lookUp
   .select($"LocationID" as "PULocationID"
          ,$"Borough" as "PUBorough"
          ,$"Zone" as "PUZone"
          ,$"service_zone" as "PUService_Zone"
         )
   .join(streamData,"PULocationID")
 ) 

val joinedData = (
    lookUp
   .select($"LocationID" as "DOLocationID"
          ,$"Borough" as "DOBorough"
          ,$"Zone" as "DOZone"
          ,$"service_zone" as "DOService_Zone" 
         )   
   .join(joinData,"DOLocationID")
  )
display(joinedData)

// COMMAND ----------

// MAGIC %md ## Step 4: Perform predictions on streaming data 
// MAGIC 
// MAGIC This example shows how to use a previously-trained Spark MLlib model to do predictions, in real time, on streaming data. In this case we will use the Spark MLlib model that was created in the previous tutorial and stored persistently in Azure Blob Storage. The model will now be read and activated. 
// MAGIC 
// MAGIC ##### Note: The ML model predicts the 'duration' of each trip based on pickup location, dropoff location and pickup time.

// COMMAND ----------

// MAGIC %md ### Step 4.1: Transform the streaming data so that it conforms to the schema expected by the model. ###
// MAGIC 
// MAGIC The previously trained model expects the input data for which it will provide predictions to have a specific schema. So, we will have to transform the event data to match the schema expected by the model. The following transformations will be applied to the streaming data:
// MAGIC 
// MAGIC 1. typecast numeric strings to Integer, Double and Timestamp as required.
// MAGIC 
// MAGIC 2. Use SparkSQL's datetime function to extract minutes, hour, day of week, day of month and weekday from the pickup datetime.
// MAGIC 
// MAGIC 3. Create a new column called 'duration' that is the difference between the pickup and the dropoff time. We will use the `duration` column as the 'label' for prediction i.e we will perform predictions for this column using other columns as 'features'
// MAGIC 
// MAGIC 4. As we are going to use ML algorithms, we will have to convert the categorical variables in the dataset into numeric variables. We will do this by:  
// MAGIC 
// MAGIC   4.1. Indexing
// MAGIC   
// MAGIC   4.2 Encoding
// MAGIC   
// MAGIC   4.3 Assembling into Vectors
// MAGIC   
// MAGIC   4.4 Normalizing the vectors
// MAGIC   
// MAGIC   4.5 Indexing the Label
// MAGIC 
// MAGIC ##### Note: 
// MAGIC After processing/transforming the data we write the results to a <b> file </b>. We will use the file as the source of data for the ML Model. So, rather than using a real stream, we can <b> simulate a stream </b> using Spark to read the data from the file as a stream.

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StringIndexer

val trpdata = spark.read.option("header", true).csv(wasbsPath)
val data = trpdata   
         .select($"*"
                ,$"tpep_pickup_datetime".cast(TimestampType).alias("pickup_time")
                ,$"tpep_dropoff_datetime".cast(TimestampType).alias("dropoff_time")
                ,$"fare_amount".cast(DoubleType).alias("fare"))
         .drop("tpep_pickup_datetime","tpep_dropoff_datetime","","fare_amount")

val selectData = data 
               .select($"pickup_time",$"pickup_time".cast(LongType).alias("pickup")      
                      ,$"dropoff_time",$"dropoff_time".cast(LongType).alias("dropoff")
                      ,$"trip_distance".cast(DoubleType)
                      ,$"PULocationID".cast(IntegerType)
                      ,$"DOLocationID".cast(IntegerType)
                      ,$"RatecodeID".cast(IntegerType)
                      ,$"store_and_fwd_flag"
                      ,$"payment_type".cast(IntegerType)
                      ,$"fare",$"extra".cast(DoubleType))
               .withColumn("duration",unix_timestamp($"dropoff_time")-unix_timestamp($"pickup_time"))
               .withColumn("pickup_hour",hour($"pickup_time"))
               .withColumn("pickup_day",dayofweek($"pickup_time"))
               .withColumn("pickup_day_month",dayofmonth($"pickup_time"))
               .withColumn("pickup_minute",minute($"pickup_time"))
               .withColumn("pickup_weekday",weekofyear($"pickup_time")) 
               .withColumn("pickup_month",month($"pickup_time"))
               .filter($"fare">0 || $"duration">0 || $"fare"<5000)
               .drop("pickup","dropoff_time","pickup_time")


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

val encoder = new OneHotEncoderEstimator()
  .setInputCols(Array("PUid", "DOid","Ratecode","storeFlag"))
  .setOutputCols(Array("pickup_loc", "drop_loc","RatecodeFlag","storeFwdFlag"))
  .fit(indexedData)

val encodedData = encoder.transform(indexedData)


val featureCols=Array("pickup_loc","trip_distance","pickup_hour","pickup_day","pickup_day_month","pickup_minute","pickup_weekday","pickup_month","RatecodeID")
val assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val output = assembler.transform(encodedData)
val assembledData=output.select("duration","features")

val normalizedData = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .transform(assembledData)

val labelIndexer = new StringIndexer()
                  .setInputCol("duration")
                  .setOutputCol("actuals")
val transformedData = labelIndexer.fit(normalizedData).transform(normalizedData)

val transformedDataPath = "/FileStore/ml/dataset/transformed"

//write the transformed data to a file.

transformedData.repartition(100).write
  .mode("overwrite")
  .parquet(transformedDataPath)

// COMMAND ----------

// MAGIC %md ### Step 4.2: Load Random Forest Regression Model from storage###
// MAGIC The model which is trained and save on a location at databricks filesystem in the previous tutorial is loaded to get the prediction from the model.

// COMMAND ----------

import org.apache.spark.ml.regression.RandomForestRegressionModel
val rfModel = RandomForestRegressionModel.load(modelpath)

// COMMAND ----------

// MAGIC %md ### Step 4.3: Read transformed data as realtime stream###
// MAGIC The data is read as structured stream according to the specified schema. Each column is read with the appropriate datatype so that it conforms to the schema expected by the model.

// COMMAND ----------

import org.apache.spark.ml.linalg.{VectorUDT,Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType 

val schema = new StructType()
  .add(StructField("duration", LongType))
  .add(StructField("features",VectorType))
  .add(StructField("normFeatures",VectorType))
  .add(StructField("actuals", DoubleType))

val streamingData = sqlContext.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(transformedDataPath)

display(streamingData)

// COMMAND ----------

// MAGIC %md ### Step 4.4: Get predictions on streaming data
// MAGIC We will use the ML model to get prediction data in real time for each event in the stream. Note that for each event we are displaying both the predicted valued and the actual so we can compare.

// COMMAND ----------

val rfPredictions = rfModel.transform(streamingData)
                           .select($"actuals".cast(IntegerType),$"prediction".cast(IntegerType))
 .groupBy($"prediction",$"actuals").agg(abs($"prediction"-$"actuals").as("diff")).orderBy($"diff".asc).drop($"diff")
display(rfPredictions)

// COMMAND ----------

// DBTITLE 1,To Stop the streaming
spark.stop()
