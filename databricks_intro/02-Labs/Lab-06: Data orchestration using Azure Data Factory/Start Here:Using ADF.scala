// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ## About this Tutorial
// MAGIC 
// MAGIC ### Goal:
// MAGIC The goal of this tutorial is to help you understand how Azure Data Factory (ADF) can be used with Azure Databricks, to create and automate piplines. In this tutorial, we will log into Azure portal to create a pipeline in Azure Data Factory that automates the notebooks in Azure Databricks. We'll do this by passing parameters via Azure Data Factory to the Databricks notebooks for execution.
// MAGIC 
// MAGIC ### Learnings: We'll learn the following:
// MAGIC 
// MAGIC   - An overview of ADF, how it works and it key components.
// MAGIC   - How to linking different services to ADF.
// MAGIC   - How to integrating Azure Databricks with ADF.
// MAGIC   - How to create an end-to-end Machine Learning pipeline using Databricks & ADF.
// MAGIC   - How to triggering a pipeline.
// MAGIC   - How to monitoring the status and result of the pipeline.

// COMMAND ----------

// DBTITLE 0,What is Azure Data Factory?
// MAGIC %md
// MAGIC ## What is Azure Data Factory (ADF)?
// MAGIC ![Azure Data Factory work flow](https://docs.microsoft.com/en-us/azure/data-factory/media/introduction/big-picture.png)
// MAGIC 
// MAGIC ADF is a cloud-based data integration service that orchestrates and automates the movement and transformation of data. You can create data integration solutions using ADF that can ingest data from various data stores, transform/process the data, and publish the result to data stores.
// MAGIC 
// MAGIC With ADF you can:
// MAGIC 
// MAGIC - Create and schedule data-driven workflows (called pipelines) that can ingest data from disparate data stores.
// MAGIC 
// MAGIC - Process or transform the data by using compute services such as Azure HDInsight Hadoop, Spark, Azure Data Lake Analytics, and Azure Machine Learning.
// MAGIC 
// MAGIC - Publish output data to data stores such as Azure SQL Data Warehouse for business intelligence (BI) applications to consume.
// MAGIC 
// MAGIC #### Versions: 
// MAGIC 
// MAGIC Currently, there are two versions of the service: <b>version 1</b> and the <b>current version</b>. You can learn about the differences here: https://docs.microsoft.com/en-us/azure/data-factory/compare-versions.
// MAGIC 
// MAGIC In this lab we'll use the current version.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Azure Data Factory Key components
// MAGIC 
// MAGIC Azure Data Factory is composed of four key components. These components work together to provide the platform on which we can compose data-driven workflows with steps to move and transform data.
// MAGIC 
// MAGIC #### Pipeline
// MAGIC 
// MAGIC A data factory might have one or more pipelines. A pipeline is a logical grouping of activities that performs a unit of work. Together, the activities in a pipeline perform a task. For example, a pipeline can contain a group of activities that ingests data from an Azure blob, and then runs a Hive query on an HDInsight cluster to partition the data.The benefit of this is that the pipeline allows you to manage the activities as a set instead of managing each one individually. The activities in a pipeline can be chained together to operate sequentially, or they can operate independently in parallel.
// MAGIC 
// MAGIC #### Activity
// MAGIC 
// MAGIC Activities represent a processing step in a pipeline. For example, we might use a copy activity to copy data from one data store to another data store. Similarly, you might use a Hive activity, which runs a Hive query on an Azure databricks spark cluster, to transform or analyze your data. Data Factory supports three types of activities: data movement activities, data transformation activities, and control activities.
// MAGIC 
// MAGIC #### Datasets
// MAGIC 
// MAGIC Datasets represent data structures within the data stores, which simply point to or reference the data you want to use in your activities as inputs or outputs.
// MAGIC 
// MAGIC 
// MAGIC #### Linked services
// MAGIC 
// MAGIC Linked services are much like connection strings, which define the connection information that's needed for Data Factory to connect to external resources.A linked service defines the connection to the data source, and a dataset represents the structure of the data.An Azure Storage-linked service specifies a connection string to connect to the Azure Storage account. Additionally, an Azure blob dataset specifies the blob container and the folder that contains the data.
// MAGIC 
// MAGIC Linked services are used for two purposes in Data Factory:
// MAGIC 
// MAGIC - To represent a data store that includes, but isn't limited to, an on-premises SQL Server database, Oracle database, file share, or Azure blob storage account. For a list of supported data stores
// MAGIC 
// MAGIC - To represent a compute resource that can host the execution of an activity. For example, the HDInsightHive activity runs on an HDInsight Hadoop cluster. For a list of transformation activities and supported compute environments
// MAGIC 
// MAGIC 
// MAGIC #### Triggers
// MAGIC Triggers represent the unit of processing that determines when a pipeline execution needs to be kicked off. There are different types of triggers for different types of events.
// MAGIC 
// MAGIC #### Pipeline runs
// MAGIC A pipeline run is an instance of the pipeline execution. Pipeline runs are typically instantiated by passing the arguments to the parameters that are defined in pipelines. The arguments can be passed manually or within the trigger definition.
// MAGIC 
// MAGIC #### Parameters
// MAGIC 
// MAGIC Parameters are key-value pairs of read-only configuration.  Parameters are defined in the pipeline. The arguments for the defined parameters are passed during execution from the run context that was created by a trigger or a pipeline that was executed manually. Activities within the pipeline consume the parameter values.
// MAGIC 
// MAGIC A dataset is a strongly typed parameter and a reusable/referenceable entity. An activity can reference datasets and can consume the properties that are defined in the dataset definition.
// MAGIC 
// MAGIC A linked service is also a strongly typed parameter that contains the connection information to either a data store or a compute environment. It is also a reusable/referenceable entity
// MAGIC 
// MAGIC #### Control flow
// MAGIC 
// MAGIC Control flow is an orchestration of pipeline activities that includes chaining activities in a sequence, branching, defining parameters at the pipeline level, and passing arguments while invoking the pipeline on-demand or from a trigger. It also includes custom-state passing and looping containers, that is, For-each iterators.

// COMMAND ----------

// MAGIC 
// MAGIC %md
// MAGIC ## Main Lab Task: Creating A Machine Learning Pipeline 
// MAGIC 
// MAGIC In this lab, we will create a Machine Learning Pipeline.
// MAGIC 
// MAGIC - This pipeline consists of five (5) tasks each represented by an Azure Databricks notebook. 
// MAGIC - The pipeline will be automated using Azure Data Factory. 
// MAGIC 
// MAGIC The detailed explaination of each and every notebook is given below
// MAGIC 
// MAGIC ![Azure Connection ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-Workspace.png)
// MAGIC 
// MAGIC 
// MAGIC #### 1: Data Preprocessing:
// MAGIC 
// MAGIC - The data is read from Azure Blob Storage
// MAGIC 
// MAGIC - Data is preprocessed such that a Machine Learning model can be trained on it
// MAGIC 
// MAGIC - The transformed data is exported to DBFS (Databricks File System) in parquet format 
// MAGIC 
// MAGIC 
// MAGIC #### 2: Feature Transformation:
// MAGIC 
// MAGIC - Reads the transformed data from DBFS using Spark APIs
// MAGIC 
// MAGIC - Transforms categorical data using a string indexer so that it can be represented in numerical labels
// MAGIC 
// MAGIC - Assembkes transformed data into a single vector
// MAGIC 
// MAGIC - Splits the data into training and test datasets
// MAGIC 
// MAGIC - Writes both the training and test datasets back to DBFS in parquet format
// MAGIC 
// MAGIC 
// MAGIC #### 3: Model Training:
// MAGIC 
// MAGIC - Reads the training dataset from DBFS
// MAGIC 
// MAGIC - Trains the linear regression model using the training dataset
// MAGIC 
// MAGIC - Saves the model back to DBFS
// MAGIC       
// MAGIC 
// MAGIC 
// MAGIC #### 4: Prediction:
// MAGIC 
// MAGIC - Reads trained model from DBFS  
// MAGIC 
// MAGIC - Reads test dataset from DBFS
// MAGIC 
// MAGIC - Performs predictions on test dataset using the saved model
// MAGIC 
// MAGIC - Displays results of the Actual v/s Predicted values
// MAGIC 
// MAGIC 
// MAGIC #### 5: Visualization:
// MAGIC 
// MAGIC - The predicted data is read from DBFS 
// MAGIC 
// MAGIC - The results of the predicted values are displayed in a Scatter Plot Graph

// COMMAND ----------

// MAGIC %md
// MAGIC ### Six Steps: In this lab the following steps will be executed to automate the pipeline.
// MAGIC   
// MAGIC #### 1. Step1: Create an ADF instance
// MAGIC 
// MAGIC #### 2. Step2: Create an Azure Databricks Linked Service 
// MAGIC 
// MAGIC #### 3. Step 3: Author the pipeline
// MAGIC 
// MAGIC #### 4. Step 4: Trigger a pipeline run
// MAGIC 
// MAGIC #### 5. Step 5: Monitor the run
// MAGIC 
// MAGIC #### 6. Visualize the output
// MAGIC 
// MAGIC </b>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Note: 
// MAGIC 
// MAGIC - If you have already ran this tutorial once and want to go through it again, simply follow the exact same steps except that when creating the ADF Service(`cmd 8`) give it a new name each time as the name should be unique. 
// MAGIC 
// MAGIC - If you want to simply build the same pipeline again (or infact modify this one or create an entirely new one), there is no need to create a seperate ADF service as each instance can build multiple pipelines. Simply go to the + symbol(`cmd 10`) and create new pipeline with a unique name.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Step 1: Create an Azure Data Factory instance
// MAGIC 
// MAGIC 
// MAGIC <b>Step 1.1:</b> Launch Microsoft Edge or Google Chrome web browser. Currently, Data Factory UI is supported only in Microsoft Edge and Google Chrome web browsers.
// MAGIC   
// MAGIC 
// MAGIC 
// MAGIC <b>Step 1.2: </b> Select <i>Create a resource</i> on the left menu, then select <i>Analytics</i>, and then select <i>Data Factory<i>.
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC ![Azure Data Factory ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-CreateADF.png)
// MAGIC 
// MAGIC 
// MAGIC  
// MAGIC <b>Step 1.3:</b> Fill in the information requested in the New data factory pane.
// MAGIC 
// MAGIC Note that the name of the Azure data factory should be globally unique. Lets name our Data Factory `learnADFforDatabricks<any serialno.>`.
// MAGIC 
// MAGIC 
// MAGIC ![Azure Data Factory ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-NewADFPane.png)
// MAGIC 
// MAGIC 
// MAGIC <b> Step 1.4:</b> After the creation is complete, we will see the Data factory page. Select the <i> Author & Monitor </i> tile to start the Data Factory UI application on a separate tab.
// MAGIC   
// MAGIC   ![Azure Data Factory work flow](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image4.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Step 2: Create an Azure Databrick linked services
// MAGIC 
// MAGIC ![Azure Connection ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-ADFGetStarted.jpg)
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC In this section, We author a Databricks linked service. This linked service contains the connection information to the Databricks cluster:
// MAGIC 
// MAGIC <b>Step 2.1:</b> On the Let's get started page, switch to the Edit tab in the left panel.
// MAGIC 
// MAGIC <b>Step 2.2:</b> Select Connections at the bottom of the window, and then select  New.
// MAGIC 
// MAGIC <b>Step 2.3:</b> In the New Linked Service window, select Compute > Azure Databricks, and then select Continue.
// MAGIC 
// MAGIC 
// MAGIC ![Azure Connection ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AzDBNewLinkedService.jpg)
// MAGIC 
// MAGIC 
// MAGIC <b>Step 2.4:</b> In the Edit Linked Service window, complete the following steps: 
// MAGIC 
// MAGIC - <b> Step 2.4.1:</b> For Name, enter the name of your Azure Databricks instance
// MAGIC 
// MAGIC - <b> Step 2.4.2:</b> Select the appropriate Databricks workspace that you will run your notebook in
// MAGIC 
// MAGIC - <b> Step 2.4.3:</b> For Domain/ Region, info should auto-populate
// MAGIC    
// MAGIC 
// MAGIC ![Linked service azure databricks ](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AzDBNewLinkedServicePane.jpg)
// MAGIC 
// MAGIC <b>Step 2.5:</b> Generate Access Token from Azure Databricks workspace
// MAGIC 
// MAGIC - <b> Step 2.5.1:</b> Go to the user setting and click on generate token
// MAGIC 
// MAGIC - <b> Step 2.5.2:</b> Enter the comment and lifetime(days)
// MAGIC 
// MAGIC - <b> Step 2.5.3:</b> Copy the generated token and use it in linked service
// MAGIC 
// MAGIC 
// MAGIC ![Token generate](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-UserSettings.jpg)
// MAGIC 
// MAGIC 
// MAGIC ![Token Generation](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-GenNewToken.jpg)
// MAGIC 
// MAGIC 
// MAGIC ![Token generation](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-GenNewTokenSuccess.jpg)

// COMMAND ----------

// MAGIC  %md 
// MAGIC ### Step 3: Author Pipeline
// MAGIC 
// MAGIC #### Step 3.1: Select the + (plus) button, and then select Pipeline.
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline1.jpg)
// MAGIC   
// MAGIC #### Step 3.2: In the Activities toolbox, expand Databricks. Drag the Notebook activity from the Activities toolbox to the pipeline designer surface.
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline2.jpg)
// MAGIC   
// MAGIC ##### - <b>Step 3.2.1:</b> Create a Data PreProcessing NoteBook Activity  
// MAGIC    
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline3.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.2.2:</b> Create A Feature Transformation NoteBook Activity
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline4.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.2.3:</b>  Create A Model Training  NoteBook Activity
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline5.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.2.4:</b> Create A  Prediction Notebook Activity
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline6.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.2.5:</b> Create a Visualization NoteBook Activity
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline7.jpg)
// MAGIC 
// MAGIC #### Step 3.3: In the properties for the Databricks Notebook activity window at the bottom, complete the following steps:
// MAGIC   
// MAGIC ##### - <b>Step 3.3.1: </b> Switch to the Azure Databricks tab.
// MAGIC 
// MAGIC ##### - <b> Step 3.3.2: </b> Select Azure Databricks LinkedService and select the  azuredatabricks2   
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline8.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.3.3:</b> Switch to the Settings tab
// MAGIC 
// MAGIC ##### - <b>Step 3.3.4:</b> Browse to select a Databricks Notebook path. Let’s create a notebook and specify the path here. 
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline9.jpg)
// MAGIC  
// MAGIC ##### - <b> Step 3.3.5: </b> Select the Databricks Notebook  specify path
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline10.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.3.6:</b> The notebook path has is now filled in. Do the same for all the other notebooks.
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline11.jpg)
// MAGIC 
// MAGIC #### Step 3.4: Terminating the cluster Through ADF
// MAGIC 
// MAGIC ##### - <b> Step 3.4.1:</b> Create a webActivity in the pipeline
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline12.jpg)
// MAGIC 
// MAGIC ##### - <b>Step 3.4.2:</b> Go to setting tab
// MAGIC 
// MAGIC ###### - <b>Step 3.4.2.1:</b> Enter the Azure portal Url in URL Box
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline13.jpg)
// MAGIC 
// MAGIC ###### - <b> Step 3.4.2.2:</b> Select the Post Method in method dropdown
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline14.jpg)
// MAGIC 
// MAGIC ###### - <b> Step 3.4.2.3:</b> Enter the name and azure databricks key
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline15.jpg)
// MAGIC 
// MAGIC ###### - <b> Step 3.4.2.4:</b> Enter the Azure Databricks Terminate Api
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline16.jpg)
// MAGIC 
// MAGIC ##### - <b> Step 3.4.3:</b> Create a Execute pipeline and call the cluster terminate pipeline 
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline17.jpg)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Final Pipeline
// MAGIC 
// MAGIC At this is stage the final pipeline should look like this.
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline18.png)
// MAGIC 
// MAGIC 
// MAGIC </n>
// MAGIC 
// MAGIC This pipeline runs our 5 Databricks notebooks in sequence before finally terminating our cluster.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Step 4: Trigger a pipeline run #
// MAGIC 
// MAGIC ###### Select Trigger on the toolbar, and then select Trigger Now.
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline20.jpg)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 5: Monitor the pipeline run 
// MAGIC 
// MAGIC ###### It takes approximately <b> 5-8 minutes </b> to run the databricks notebooks on a databricks cluster.
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline21.jpg)
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline22.jpg)
// MAGIC 
// MAGIC After 5-8 minutes the 'status' for all the notebooks changes to `succeeded`. This is the verification that all the notebooks have run successfully 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline23.jpg)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 6: Visualize the output
// MAGIC 
// MAGIC Monitoring allows us to view the outputs of our Notebooks once they are run in the Activity Runs section. For our ML pipeline, let us go ahead and see the results of the visualization notebook
// MAGIC 
// MAGIC Lets see the output of the last notebook in the pipeline, the Visualization Notebook. The visualization notebook takes in the results of our predictions and our trained model and plots them in a simple scatter plot for us.
// MAGIC 
// MAGIC ###### Step 1: Go to Visualization Activity Name and click on Output Action
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline24.jpg)
// MAGIC 
// MAGIC ###### Step 2: Output Modal Window will appear after you will click on runPageUrl 
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline25.jpg)
// MAGIC 
// MAGIC ###### Step 3: The final output can be verified in the Visualization notebook
// MAGIC 
// MAGIC 
// MAGIC ![Create a pipeline](https://nytaxidata.blob.core.windows.net/notebookimages/Lab6-AuthorPipeline26.jpg)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Conclusion: 
// MAGIC 
// MAGIC We were able to successfully automate a simple Machine Learning pipeline consist of multiple Databricks Notebooks using ADF.
