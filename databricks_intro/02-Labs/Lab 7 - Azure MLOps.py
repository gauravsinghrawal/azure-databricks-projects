# Databricks notebook source
# MAGIC %md
# MAGIC # About this Tutorial
# MAGIC 
# MAGIC ## Goal
# MAGIC 
# MAGIC The goal of this tutorial is help you understand how to use the MLOps approach to automate machine learning with Azure Databricks. MLOps is a practice for collaboration and communication between data scientists and operations professionals to help manage production ML (or deep learning) lifecycle. Similar to the DevOps or DataOps approaches, MLOps looks to increase automation and improve the quality of production ML while also focusing on business and regulatory requirements. MLOps applies to the entire lifecycle - from integrating with model generation (software development lifecycle, continuous integration/continuous delivery), orchestration, and deployment, to health, diagnostics, governance, and business metrics.
# MAGIC 
# MAGIC MLOps in Azure provided through the integration of two Azure Services:
# MAGIC 
# MAGIC 1) Azure Machine Learning Service (Azure ML Service)
# MAGIC 
# MAGIC 2) Azure DevOps
# MAGIC 
# MAGIC Azure MLOPs lets you automate a general machine learning workflow shown in the diagram:
# MAGIC 
# MAGIC 
# MAGIC ![Workflow](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-0-workflow.png)
# MAGIC 
# MAGIC ## Learning:
# MAGIC 
# MAGIC in this tutorial You will learn how to use Azure DevOps and Azure ML Service to create workflows that automate the following:
# MAGIC 
# MAGIC a) Training a machine learning model starting from code residing in a git repo. 
# MAGIC 
# MAGIC   - The ML model will be trained on a Azure Databricks cluster.
# MAGIC 
# MAGIC   - This can be automated so that the training is triggred everytime the code in the git repo.
# MAGIC   
# MAGIC   - The model will be registered with the Azure machine learning service registry  
# MAGIC 
# MAGIC b) Deploying the trained model as web service
# MAGIC 
# MAGIC   - The model will be deployed on either Azure Container Instance (ACI) or Azure Kubernetes Cluster (AKS)
# MAGIC   
# MAGIC   - This can be be automated so that deployment is triggered everytime the a new model is registered with the model registry.
# MAGIC 
# MAGIC 
# MAGIC ## Concept: Compute Target
# MAGIC 
# MAGIC In Azure a [compute target](https://docs.microsoft.com/en-us/azure/machine-learning/service/concept-compute-target) is a designated compute resource/environment where you run your training script or host your service deployment. This location may be your local machine or a cloud-based compute resource. Using compute targets make it easy for you to later change your compute environment without having to change your code. For training and deployment, Azure provides a choice of compute targets. In this tutorial we will use:
# MAGIC 
# MAGIC - Azure Databricks Cluster for as compute tagret for training.
# MAGIC 
# MAGIC - Azure Container Instance (ACI) as compute targets for deployment. (We have the option to deploy to AKS as well).

# COMMAND ----------

# MAGIC %md
# MAGIC #Prequisites
# MAGIC 
# MAGIC ##### There are two prequisites to proceed with the lab.
# MAGIC 
# MAGIC #### 1) Storage Account 
# MAGIC 
# MAGIC   ##### The Azure Blob Storage account where the NY Taxi data is stored. Configure the access in *cmd 3* below.
# MAGIC 
# MAGIC #### 2) Github Repository 
# MAGIC 
# MAGIC   ##### The github repository that contains all the code required to train and deploy an Azure Databricks ML model. 
# MAGIC 
# MAGIC ##### By default the repo is located here:https://github.com/snapAzureMlLab/azuremllab

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #Github Repository Structure
# MAGIC 
# MAGIC ## Environment Setup
# MAGIC 
# MAGIC - requirements.txt : It consists of a list of python packages which are needed by the train.py to run successfully on host agent (locally).
# MAGIC 
# MAGIC - set-environment-vars.sh : This script prepares the python environment i.e. install the Azure ML SDK and the packages specified in requirements.txt
# MAGIC 
# MAGIC - environment.yml :  build agent containing Python 3.6 and all required packages.
# MAGIC 
# MAGIC - setup.py : Importing the set of package for model
# MAGIC 
# MAGIC ## Pipelines
# MAGIC 
# MAGIC - /azdo_pipelines/base-pipeline.yml : a pipeline template used by build-pipeline. It contains steps performing linting, data and unit testing.
# MAGIC 
# MAGIC - /azdo_pipelines/build-pipeline.yml : a pipeline triggered when the code is merged into master. It performs linting,data integrity testing, unit testing, building and publishing an ML pipeline.
# MAGIC 
# MAGIC ## cluster_config
# MAGIC 
# MAGIC - cluster_config/cluster.py : Databricks Cluster config for creating a new cluster and terminate or delete cluster.
# MAGIC 
# MAGIC - cluster_config/cluster_manager.py : Use Existing Databricks cluster and terminate the cluster. 
# MAGIC 
# MAGIC - library.json : install the library azure - ml - sdk in databricks cluster 
# MAGIC 
# MAGIC ## Source Code
# MAGIC 
# MAGIC - /src/train/train.py : a training step of an ML training pipeline.
# MAGIC 
# MAGIC - /aml_service/pipelines/train_pipeline.py : Create a pipeline in azure machine learning for training model. 
# MAGIC 
# MAGIC - /aml_service/experiment/experiment.py : Create a experiment for Tracking the model 
# MAGIC 
# MAGIC - /aml_service/experiment/register_model.py : Registers a  trained mode into azure machine learning service.
# MAGIC 
# MAGIC - /aml_service/experiment/workspace.py  : Connect the azure machine learning workspace with azure devops.
# MAGIC 
# MAGIC - /aml_service/experiment/attach_compute.py : select the Target the cluster compute (databricks) for training the model
# MAGIC 
# MAGIC ## Scoring
# MAGIC 
# MAGIC - /src/score/score.py : a scoring script which is about to be packed into a Docker Image along with a model while being deployed to QA/Prod environment.
# MAGIC 
# MAGIC - /src/score/conda_dependencies.yml : contains a list of dependencies required by sore.py to be installed in a deployable Docker Image
# MAGIC 
# MAGIC - /src/score/inference_config.yml, deployment_config_aci.yml, deployment_config_aks.yml : configuration files for the AML Model Deploy pipeline task for ACI and AKS deployment targets.

# COMMAND ----------

# MAGIC %md
# MAGIC # Tasks: 
# MAGIC 
# MAGIC ### In this tutorial you will be performing the following sequence of tasks: ###
# MAGIC 
# MAGIC Task 1: Create an Azure ML Service workspace 
# MAGIC 
# MAGIC Task 2: Sign up and create an Azure DevOps Project
# MAGIC 
# MAGIC Task 3: Import the existing git repo into the Azure DevOps Project 
# MAGIC  
# MAGIC Task 4: Create Databricks cluster (if does not exist)
# MAGIC    
# MAGIC   - Task 4.1: Create Databricks Cluster
# MAGIC   
# MAGIC   - Task 4.2: Get Databricks Cluster Id
# MAGIC 
# MAGIC   - Task 4.3: Get Databricks Access token 
# MAGIC 
# MAGIC Task 5: Train the model on an Azure Databricks Cluster. 
# MAGIC 
# MAGIC   - Task 5.1: Create a service principle for the resource group
# MAGIC   
# MAGIC   - Task 5.2: Setup the training pipeline.
# MAGIC   
# MAGIC   - Task 5.3: Configure Variables
# MAGIC   
# MAGIC   - Task 5.4: Execute the pipeline for training model
# MAGIC   
# MAGIC Task 6: Deploy the trained model as a web service.
# MAGIC 
# MAGIC   - Task 6.1:  Install the Azure Machine Learning extension from the marketplace to the Azure DevOps account.
# MAGIC   
# MAGIC   - Task 6.2: Set up the Service Connection 
# MAGIC   
# MAGIC   - Task 6.3: Create a pipeline to deploy to Azure Container Instance (ACI)
# MAGIC   
# MAGIC   - Task 6.4: Trigger the Pipeline    
# MAGIC   
# MAGIC   - Task 6.5: Do some predictions to determine the performance (accuracy) of the model using RMSE 
# MAGIC   
# MAGIC Task 7: Retrain and redeploy the model 
# MAGIC   
# MAGIC   - Task 7.1: Update the source file in the repo to trigger the retraining and redeployment pipeline
# MAGIC 
# MAGIC   - Task 7.2: Check that pipelines succesfully executed
# MAGIC   
# MAGIC   - Task 7.3: Check that new model was indeed created
# MAGIC   
# MAGIC   - Task 7.4: Compare the performance/accuracy of the new model vs the previous one using the RMSE score
# MAGIC   
# MAGIC Task 8 : Resource Cleanup - Delete the Azure Container Instance after you are done with the Lab.
# MAGIC   
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Task 1: Create an Azure ML service workspace
# MAGIC 
# MAGIC ## Task 1.1: 
# MAGIC 
# MAGIC Create an Azure ML service workspace as described here: https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-manage-workspace.
# MAGIC 
# MAGIC You will be asked to provide an Azure Resource Group Name and Workspace name. In this tutorial, we use:
# MAGIC 
# MAGIC 1) Azure Resource Group Name: <b> AzureDBLab7 </b>
# MAGIC 
# MAGIC 2) Workspace Name: <b> Lab7Workspace1 </b>
# MAGIC 
# MAGIC After succesful creation of the workspace you should see the following screen:
# MAGIC   
# MAGIC ## Task 1.2 
# MAGIC 
# MAGIC   Download the <b> config.json </b> file as instructed in the link above.
# MAGIC   
# MAGIC   ![workspace](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-1-configure.png)
# MAGIC   
# MAGIC   The file has the following 3 pieces of information:
# MAGIC   
# MAGIC 1) Azure subscription Id 
# MAGIC   
# MAGIC 2) Resource group name 
# MAGIC   
# MAGIC 3) Workspace name
# MAGIC 
# MAGIC Note down this information as it is needed later while setting up the training pipeline (Task 5.3.5)
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Task 2: Create a Azure DevOps Project
# MAGIC 
# MAGIC Sign into Azure DevOps and create a DevOps project as described here https://azure.microsoft.com/en-us/services/devops/ 
# MAGIC 
# MAGIC You will be asked to provide a name for the project. In this tutorial we use <b> lab7Project </b> 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Task 3: Import an existing repo into Azure Devops
# MAGIC 
# MAGIC Import the git repository into Azure DevOps as described here:  https://docs.microsoft.com/en-us/azure/devops/repos/git/import-git-repository?view=azure-devops
# MAGIC 
# MAGIC You should be able to see the imported git repo in your DevOps account.
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-2-azureRepo.png)
# MAGIC 
# MAGIC 
# MAGIC ### Note: Project Environment Variables
# MAGIC 
# MAGIC To run either locally or on Azure DevOps, this set of scripts need a few environment variables to be set.  These are set in the <b> set-environment-vars.sh </b> file in the repo. The values are obtained from the Azure DevOps variable list that will be populated in Task 4.3.1. 
# MAGIC 
# MAGIC Here's the list of the relevant variables.
# MAGIC 
# MAGIC ### Cluster Management Environment Variables
# MAGIC 
# MAGIC - DATABRICKS_DOMAIN: The Databricks instance name 
# MAGIC 
# MAGIC - DATABRICKS_ACCESS_TOKEN: The Databricks Personal Access Token you have generated on your workspace 
# MAGIC 
# MAGIC - DATABRICKS_CLUSTER_NAME_SUFFIX: Optional. A suffix you can use to identify all of your clusters. If you choose not to set one, all of your clusters will be called aml-cluster.
# MAGIC 
# MAGIC - DATABRICKS_CLUSTER_ID : Use this variable if you prefer to use an existing cluster to run your train pipeline.
# MAGIC 
# MAGIC ## Train Environment Variables
# MAGIC 
# MAGIC - AML_WORKSPACE_NAME: The name of your Azure ML service workspace. Amongst all the generated infrastructure resources, this will have the -AML-WS suffix.
# MAGIC 
# MAGIC - RESOURCE_GROUP: The name of the Azure Resource Group that contains all of your workloads (Databricks Workspace, Azure ML Service, etc.). Amongst all the generated infrastructure resources, this will have the -AML-RG suffix.
# MAGIC 
# MAGIC - SUBSCRIPTION_ID: The ID of the Azure Subscription being used to run this infrastructure.
# MAGIC 
# MAGIC - TENANT_ID: The ID of the Azure Active Directory tenant associated with the Azure Subscription
# MAGIC 
# MAGIC - SP_APP_ID: The Application ID of the Service Principal to run this code.
# MAGIC 
# MAGIC - SP_APP_SECRET: The Application Secret (or password) of the Service Principal to run this code.
# MAGIC 
# MAGIC - SOURCES_DIR: The root folder of this code. If you're associating this repo with an Azure DevOps pipeline without much customization when you clone the repo, this variable can set as $(Build.SourcesDirectory).
# MAGIC 
# MAGIC - TRAIN_SCRIPT_PATH: If you use this code structure as is, the current train script path is <b> src/train/train.py </b>
# MAGIC 
# MAGIC - DATABRICKS_WORKSPACE_NAME: The name that was given to your Azure Databricks Workspace. Amongst all the generated infrastructure resources, this will have the -AML-ADB suffix.
# MAGIC 
# MAGIC - DATABRICKS_COMPUTE_NAME_AML: The name you want to give to the compute target that will be associated with the Azure ML Service Workspace. This will be used to attach the Azure Databricks cluster to the Azure ML service workspace.
# MAGIC 
# MAGIC - MODEL_DIR: The directory to save the trained model to, on the Databricks Cluster. It's recommended to use /dbfs/model.
# MAGIC 
# MAGIC - MODEL_NAME: The name to give to the trained model. For example: `my-trained-model`. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 4: Create Databricks cluster
# MAGIC 
# MAGIC An Azure Databricks cluster is needed to train the ML model. If a cluster has NOT already been created for you then create the cluster as described in Task 4.1.
# MAGIC 
# MAGIC Either way note down the following 4 pieces of information as it will be needed later. If someone provisioned the cluster for you, get the information from them. If not follow the steps in **Task 4.1**.
# MAGIC 
# MAGIC 1) **Databricks Workspace name:** In this tutorial we are using **AzureDBLab7Workspace** 
# MAGIC 
# MAGIC 2) **Databricks domain:** In this tutorial we are using **eastus2.azuredatabricks.net**
# MAGIC 
# MAGIC 3) **Databricks ID**
# MAGIC 
# MAGIC 4) **Databricks Access Token**. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4.1 Create a Databricks Cluster (if one has not already been provisioned for you)
# MAGIC 
# MAGIC ### Task 4.1.1 In the Databricks console, click the `Clusters` tab and then click on `Create Cluster`
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-37-cluster.png)
# MAGIC 
# MAGIC ### Task 4.1.2 Provide the cluster name and click on `Create cluster`
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-38-clusterconfig.png)
# MAGIC 
# MAGIC ## Task 4.2: Get Databricks cluster id
# MAGIC 
# MAGIC 
# MAGIC Get the Cluster ID from  `Advanced Options` -> `Tags`
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-10-clusterids.png)
# MAGIC 
# MAGIC ## Task 4.3: Get Databricks Access token
# MAGIC 
# MAGIC ### Task 4.3.1
# MAGIC 
# MAGIC On the main Databricks console Select the `User Settings` on the top lefft 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-11-userSetting.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 4.3.2
# MAGIC 
# MAGIC Select the `Access Tokens` -> `Generate New Token`
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-12-usersetting.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 4.3.3
# MAGIC 
# MAGIC Enter a comment if you want and then select `Generate`
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-13-Validity.png)
# MAGIC 
# MAGIC ### Task 4.3.4
# MAGIC 
# MAGIC Copy and note the generated token and select `Done`. 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-14-GeneratePassword.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 5: Train the model on  Azure Databricks cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5.1: Create Service Principal for Azure Resource Group
# MAGIC 
# MAGIC Please note down the following three values after creating the service principal. We will need them in subsequent steps:
# MAGIC 
# MAGIC 
# MAGIC   - <b> Application (client) ID </b>
# MAGIC   - <b> Directory (tenant) ID </b>
# MAGIC   - <b> Client Secret </b>
# MAGIC 
# MAGIC   
# MAGIC <b> Note: </b> You must have sufficient permissions to register an application with your Azure AD tenant, and assign the application to a role in your Azure subscription. Contact your subscription administrator if you don't have the permissions. Normally a subscription admin can create a Service principal and can provide you the details.
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.1: 
# MAGIC In the main Azure Portal select `Azure Active Directory`
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-07-AD.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.2: 
# MAGIC Select the `App Registration`  
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-8-AppRegistration.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.3:
# MAGIC Select `New Registration` 
# MAGIC 
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-8-newRegistration.jpg)
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.4: 
# MAGIC Enter the display name and click `Register`
# MAGIC 
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-9-Display.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.5:
# MAGIC After the registration is complete, navigate to the overview pane for the App and copy the `Directory (tenant) ID` and `Application (client) ID`. Note it down for later use.
# MAGIC 
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-10-SpDes.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.1.6: 
# MAGIC Select `Certificates and Secrets`
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-11-SpCertifictes.png)
# MAGIC 
# MAGIC ### Task 5.1.7: 
# MAGIC Click on `Client Secrets` -> `New client secret` to note down the secret for later use.
# MAGIC 
# MAGIC ![Resource Group ID](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-12-SPCS.png)
# MAGIC 
# MAGIC ### Task 5.1.8:
# MAGIC Go to the Resource Group where you have created the Azure ML Workspace Service and assign `Owner` role to the Service Principle. Click on `Access Control`>`Check access`>`Add`> Type in name of service principle > `Save`.
# MAGIC 
# MAGIC ![Role Assignment](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlops-role-assign.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 5.2 Attach 'Compute' to our Azure ML Workspace #
# MAGIC 
# MAGIC In this Lab, we will be using Azure Databricks as our computation target in order to train our models. For this, we need to add our Azure Databricks workspace as a `compute target` in Azure ML Workspace.
# MAGIC 
# MAGIC - Go to Azure ML Workspace and select `Compute` under Assets.
# MAGIC - Go to `Attached Compute`>`New`
# MAGIC - Select Azure Databricks as the compute type. Name of the compute should be `ABD-Compute`. Select the Databricks workspace and associated token created in Task 4.
# MAGIC 
# MAGIC ### Note : The Databricks workspace and the Azure ML Workspace should be part of the same resource-group in order to complete this step.
# MAGIC 
# MAGIC ![compute](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/attach-compute1.png)
# MAGIC 
# MAGIC ### The compute name should show a green checkmark if its successfully created.
# MAGIC 
# MAGIC ![compute](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/attach-compute2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5.3: Create an Azure DevOps 'Build' Pipeline
# MAGIC 
# MAGIC Create a **Azure DevOps Build pipeline ** that will ** train ** and ** register ** the ML model.
# MAGIC 
# MAGIC The pipeline will use an existing Databricks cluster from Task 4 above to serve as a remote compute for training the model using Azure ML Service.
# MAGIC 
# MAGIC 
# MAGIC ### Task 5.3.1
# MAGIC 
# MAGIC - #### In Azure DevOps, go to `Pipelines > Create Pipeline`. 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/build-pipeline1.PNG)  
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ### Task 5.3.2
# MAGIC 
# MAGIC - #### Choose `Azure Repos Git` as the location of the code.
# MAGIC 
# MAGIC   
# MAGIC   
# MAGIC  ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-4-build-connect.png)
# MAGIC  
# MAGIC  </n>
# MAGIC 
# MAGIC ### Task 5.3.4
# MAGIC 
# MAGIC - #### In `Select a repository`, from the list of repos displayed, select the repo imported from github in cmd TASK 3
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-13-importbuild.png)
# MAGIC 
# MAGIC ### Task 5.3.5
# MAGIC 
# MAGIC - #### Select `Existing Azure Pipelines YAML` file
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-5-existing-yaml.png)
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ### Task 5.3.6
# MAGIC 
# MAGIC - #### Specify `/azdo_pipelines/build-train.yml` for branch and file. and Enter the `Continue` 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-2-2.png)
# MAGIC 
# MAGIC 
# MAGIC <b> You should see the contents of the YAML file displayed as shown here: </b>
# MAGIC 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-3-3.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Task 5.4: Configure Pipeline Variables.  
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC - Click on **`Variables`**
# MAGIC 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidata.blob.core.windows.net/notebookimages/Lab7/Lab7-4-4.png)
# MAGIC 
# MAGIC   
# MAGIC   -----------------
# MAGIC   
# MAGIC   On **Pipeline variables**, add the following <b> ten </b> variables and values. 
# MAGIC   
# MAGIC   #### To obtain the values for the next four Azure Databricks-related variables see Task 4.1
# MAGIC   
# MAGIC    1)  `DATABRICKS_WORKSPACE_NAME`
# MAGIC 
# MAGIC    2) `DATABRICKS_DOMAIN`
# MAGIC   
# MAGIC    3) `DATABRICKS_ACCESS_TOKEN`
# MAGIC 
# MAGIC    4) `DATABRICKS_CLUSTER_ID`
# MAGIC   
# MAGIC    #### To obtain the values for the next these three variables see Task 1.2
# MAGIC     
# MAGIC    5) `AML_WORKSPACE_NAME`
# MAGIC 
# MAGIC    6) `RESOURCE_GROUP`
# MAGIC 
# MAGIC    7) `SUBSCRIPTION_ID`
# MAGIC   
# MAGIC   ### To obtain the values for the next three variables, see Task 5.1  
# MAGIC   
# MAGIC    8) `TENANT_ID`
# MAGIC 
# MAGIC    9) `SP_APP_ID`
# MAGIC 
# MAGIC    10) `SP_APP_SECRET`
# MAGIC   
# MAGIC </n> 
# MAGIC 
# MAGIC <b> After adding all the variables make sure to "SAVE" them.  Then you should be able to see the variables pane as follows: </b>
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidata.blob.core.windows.net/notebookimages/Lab7/Lab7-6-6.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Task 5.5: Execute pipeline to train model
# MAGIC 
# MAGIC ##### Make sure that the databricks cluster is "running". Or else the execution will fail. ####
# MAGIC ##### Time duration taken by the build pipeline to run completely should be around 5 minutes. ###
# MAGIC 
# MAGIC Click on **RUN**. 
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-7-7.png)
# MAGIC 
# MAGIC Once the pipline is finished, you can check the status through the execution logs:
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-8-8.png)
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC You can now see that the model has been created. Note down the the <b> version number </b> and <b> time of creation </b>.
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-16-modelVersion.png)
# MAGIC 
# MAGIC 
# MAGIC </n> <b> NOTE </b>
# MAGIC   
# MAGIC If the pipeline fails to execute, double check that the databricks cluster is running. 
# MAGIC   

# COMMAND ----------

# MAGIC %md 
# MAGIC # Task 6. Deploy the Model
# MAGIC 
# MAGIC The following sequence of steps will deploy the model as a web service hosted in an Azure Container Instance (ACI).
# MAGIC 
# MAGIC ### To build the correct environment for ACI , we have to provide the following four components:
# MAGIC 
# MAGIC - A scoring script to show how to use the model.
# MAGIC - An environment file to show what packages need to be installed.
# MAGIC - A configuration file to build the container instance.
# MAGIC - A trained model
# MAGIC 
# MAGIC ### Define your Deployment configuration 
# MAGIC 
# MAGIC Before deploying, you must define the deployment configuration. The deployment configuration is specific to the compute target that will host the web service. You can find deployment configuration file for ACI in the Azure Repo @ **'src/Score/deployment_config_aci.yml**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6.1:  Install Azure Machine Learning extension
# MAGIC 
# MAGIC Install the Azure Machine Learning extension from the [marketplace](https://marketplace.visualstudio.com/acquisition?itemName=ms-air-aiagility.vss-services-azureml) to the desired Azure DevOps account.
# MAGIC 
# MAGIC Specify an organization to which you have the permissions to install the extension.
# MAGIC 
# MAGIC ![Releasepipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-9-MarketPlace.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC  ## Task 6.2: Set up the Service Connection:
# MAGIC 
# MAGIC You have to create a Service Connection to provide access to the Azure ML artifacts.
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ### Task 6.2.1 
# MAGIC 
# MAGIC - Select your project. 
# MAGIC 
# MAGIC - Go to `Project Settings`->`Service connections` -> `Azure Resource Manager`.
# MAGIC 
# MAGIC ![Releasepipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-9-overview.png)
# MAGIC 
# MAGIC 
# MAGIC </n> 
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ### Task 6.2.2
# MAGIC 
# MAGIC 1) Provide a `Connection name` for the connection. In this tutorial it is **Lab7DevOpsServiceConnection1** 
# MAGIC 
# MAGIC 2) For `Scope level` select **AzureMLWorkspace**
# MAGIC 
# MAGIC 3) Pick the appropriate choice from the drop down menu for the `Resource Group` and `Machine Learning Workspace`
# MAGIC 
# MAGIC </n>
# MAGIC 
# MAGIC ![Releasepipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-14-serviceConnection.png)

# COMMAND ----------

# MAGIC   %md
# MAGIC 
# MAGIC ## Task 6.3: Create a Release pipeline
# MAGIC 
# MAGIC The following sections will create an **Azure DevOps Release** pipeline that will deploy the trained model to Azure Container Instance (ACI). [Note, you can deploy to Azure Kubernetes Services (AKS) as well if you want. In the real-world, users typically deploy to ACI for QA and to AKS for production. However, in this tutorial we will only deploy to ACI]
# MAGIC 
# MAGIC The Release pipeline is triggered **every time a new model is registered** in the Azure Machine Learning Service workspace. The final flow after the pipeline is created will appear as shown in the figure below.
# MAGIC 
# MAGIC ##### The pipeline run will take about ~15 minutes to complete. You can see and track the progress of the run.
# MAGIC 
# MAGIC ![Releasepipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlops_release1.png)
# MAGIC 
# MAGIC 
# MAGIC ### Task 6.3.1: Create a new "Release Pipeline"
# MAGIC 
# MAGIC #### Task 6.3.1.1: Go to `Pipeline -> Releases -> New pipeline`  
# MAGIC 
# MAGIC ![Release Pipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/release2.PNG)
# MAGIC 
# MAGIC 
# MAGIC #### Task 6.3.1.2:  Click on start with an `Empty job`    
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/release3.png)
# MAGIC 
# MAGIC #### Task 6.3.1.4: Change the Stage name `Deploy to ACI`. We can also rename the pipeline by clicking on `New Release Pipeline`. Here, we call our pipeline `MLOps-Databricks-Release`
# MAGIC 
# MAGIC   ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/release4.png)
# MAGIC 
# MAGIC 
# MAGIC #### Task 6.3.2: Now add two artifacts to the release pipeline:  
# MAGIC 
# MAGIC 1. Azure Repo artifact, and 
# MAGIC 
# MAGIC 2. AzureML Model artifact.
# MAGIC 
# MAGIC #### Task 6.3.2.1: Select `+Add` to add the Azure Repo artifact
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/release5.png)
# MAGIC 
# MAGIC 
# MAGIC #### Task 6.3.2.2: Add the associated `Azure Repo` artifact and fill in the associated form with the appropriate values
# MAGIC 
# MAGIC ![GitHub Artifact](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-25-repoart.PNG)
# MAGIC 
# MAGIC #### Task 6.3.2.3: Select `+ Add` again to add the `AzureML Model` artifact and fill in the associated form with appropriate values  
# MAGIC  
# MAGIC  ![AzureMLArtifact](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-26-mlart.PNG)
# MAGIC 
# MAGIC 
# MAGIC #### Task 6.3.2.4: Enable the `Continuous deployment trigger` 
# MAGIC 
# MAGIC This ensures that the release pipeline will be triggered every time a new version of the model is registered in the Azure Machine Learning workspace
# MAGIC 
# MAGIC ![Azure Machine Learning services](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-22-TriggerEnable.PNG)
# MAGIC 
# MAGIC ### Task 6.3.3: Add a task to the `Deploy to ACI` stage
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-21-mlopsDatabricks.png)
# MAGIC 
# MAGIC #### Task 6.3.3.1:  Go to `Tasks` tab. Click on the `+` sign in the `Agent job` section    
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-33-agentjob.png)
# MAGIC 
# MAGIC #### Task 6.3.3.2: Select the `AzureML Model Deploy` task
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-34-model.png)
# MAGIC 
# MAGIC #### Task 6.3.4: Fill in the task details.
# MAGIC 
# MAGIC The `Azure ML Deploy` task will show this message `Some settings need attention`
# MAGIC 
# MAGIC ![mlops](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/mlops-release-need-attention.png)
# MAGIC 
# MAGIC #### Task 6.3.4.1: Click on the label to enter parameters required by the task.
# MAGIC 
# MAGIC Use the task parameters shown in the table below:
# MAGIC 
# MAGIC ![Releasepipeline](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-18-docs.png)
# MAGIC 
# MAGIC #### Task 6.3.4.2: Fill in the `Model Information` section
# MAGIC 
# MAGIC For `Inference config Path`,select the Azure repo path as shown.
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab-36-Infrence.png)
# MAGIC 
# MAGIC #### Task 6.3.4.3: Fill in the `Deployment Information` section
# MAGIC 
# MAGIC For the `Deployment config file`, select the path from Azure Repo as shown.
# MAGIC 
# MAGIC Check the `Overwrite existing deployment` checkbox. This is so that we can use the same ACI instance every time the release pipeline is triggered. Otherwise a new ACI instance is created for every pipeline run.
# MAGIC 
# MAGIC ##### NOTE : The Deployment name we give here will create an ACI service of the same name inside your resource group. Later in Task 8, we will delete this service as part of our cleanup.
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-17-development.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 6.4 : Trigger the Pipeline
# MAGIC 
# MAGIC #### Save the pipeline and trigger it manually. After the pipeline run complete succesfully you can see the deployment in the Azure ML workspace.
# MAGIC 
# MAGIC ### Task 6.4.1 `Save` the Pipeline  
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-31-save.PNG)
# MAGIC 
# MAGIC ### Task 6.4.2: To trigger the pipeline, click on `Create Release`.
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-32-Create.png)
# MAGIC 
# MAGIC ### Task 6.4.4: Select the `Create` icon at the bottom.
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/release8.png)
# MAGIC 
# MAGIC ### Task 6.4.5: ACI Release pipeline process has now begun. We can click on the release number to check its status.
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/release10.png)
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/release6.PNG)
# MAGIC 
# MAGIC ### Task 6.4.6: For more information, we can view the Logs for the `Deploy to ACI` stage. 
# MAGIC 
# MAGIC Note: Ignore the 3 warning messages displayed during the release pipeline run.
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-33-Excute.png)
# MAGIC 
# MAGIC ### In the Azure Portal you can see that the model is deployed
# MAGIC 
# MAGIC ![cdtrigger](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-34-modeDeploy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Azure ML Studio to track the progress of various steps. ###
# MAGIC 
# MAGIC In all the previous steps, we have been using Azure ML service for tasks like attaching compute, model training and registering; but we were triggering these steps via the Azure DevOps service as we want to integrate these two for enabling CI/CD. But we can also see the status of your workspace using the **Azure ML Studio** interface.
# MAGIC 
# MAGIC ### Task Go to your Azure ML Workspace service and in `Overview` tab click on `Launch the Azure ML Studio` button.
# MAGIC ![mlstudio1](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/azureml_launch.PNG)
# MAGIC 
# MAGIC ### Previously, in step 5.2, we attached Azure Databricks as 'Compute Target'. You can see this in the `Compute > Attached Compute` tab.
# MAGIC ![mlstudio1](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlstudio_compute.PNG)
# MAGIC 
# MAGIC ### You can see the list of all the runs in the `Pipelines > Pipeline runs` tab. It displays some useful information for each run.
# MAGIC ![mlstudio1](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlstudio_buildpipeline.PNG)
# MAGIC 
# MAGIC ### You  can list of deployed models in the `Endpoints > Real-time endpoints` tab
# MAGIC ![mlstudio1](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlstudio_deploy1.PNG)
# MAGIC 
# MAGIC ### You can find more details about a deployed model such as its status, url etc.
# MAGIC 
# MAGIC ![mlstudio1](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/mlstudio_deploy2.PNG)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Task 6.5: Predict with the deployed model and determine accuracy/performance
# MAGIC 
# MAGIC We will now do some inferences againts the deployed model by invoking its web service. 
# MAGIC 
# MAGIC We will use the  <b> Root-Mean-Square Error (RMSE) </b> score to determine the accuracy (or performance) of the model. RMSE is one of the commonly used scores to determine model accuracy. 
# MAGIC 
# MAGIC Run the following cells to get predictions on the deployed model.

# COMMAND ----------

# DBTITLE 1,Check SDK version 
import azureml.core
# Check core SDK version number - based on build number of preview/master.
print("SDK version:", azureml.core.VERSION)

# COMMAND ----------

# DBTITLE 1,Authenticate Azure subscription and create workspace
#Define workspace params(Parameters) - You can get these values for the variables from the config file in Task 1.2

subscription_id = "ccd791bb-d562-46d5-9361-326c2e57ed2f" #you should be owner or contributor
resource_group = "delmeresourcegroup" #you should be owner or contributor
workspace_name = "delmemlworkspace" #your workspace name
workspace_region = "eastus2" #your region

# import the Workspace class and check the azureml SDK version
# exist_ok checks if workspace exists or not.

from azureml.core import Workspace

ws = Workspace.create(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group, 
                      location = workspace_region,
                      exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Get workspace details
ws.get_details()

# COMMAND ----------

# DBTITLE 1,Get the web service end-point URI  (Uniform Resource Identifier)
from azureml.core.webservice import Webservice
service_name = "lab7acideployment" # this should be name of the ACI service which you deployed in Task 6.3.3.7
service = Webservice(workspace=ws, name = service_name)
print(service.scoring_uri)

# COMMAND ----------

# DBTITLE 1,Read test dataset from Azure Blob
#Storage Account Acces
storage_path = "fs.azure.account.key.nytaxidataset.blob.core.windows.net"
Storageaccess_token = "f6SaevLhJ+KPSIiUSHzw5we2NYZjTlqRt6WstMaVei+VTyZmJjvzkB8B/6UKuOs65gNlHoXRSDBTe5g2lmtp3g=="
blob_path =  "wasbs://taxidata@nytaxidataset.blob.core.windows.net/azure-ml/"
test_data =  "Azure_ml_test.parquet"
spark.conf.set(storage_path,Storageaccess_token)
test_path = blob_path + test_data
test = spark.read.parquet(test_path)

# COMMAND ----------

# DBTITLE 1,Convert test data to JSON before sending to the web service
# #prediction using updated services

import json
from pyspark.sql.functions import *

data_test = test.groupBy('actuals').agg(first('duration').alias('duration'),first('features').alias('features'),first('normFeatures').alias('normFeatures')).limit(10)

test_json = json.dumps(data_test.toJSON().collect())

print(test_json)

# COMMAND ----------

# DBTITLE 1,Predict using the web service 
#selecting actuals and predicted values and creating prediction dataframe

service = Webservice(workspace=ws, name=service_name)
actuals = data_test.select('actuals').rdd.flatMap(lambda x: x).collect()
predicted = service.run(input_data=test_json)
predicted = [float(i) for i in predicted]
prediction_df = sqlContext.createDataFrame(zip(actuals, predicted), schema=['Actual', 'Predicted'])
display(prediction_df)

# COMMAND ----------

# DBTITLE 1,Get the RMSE 
from sklearn.metrics import mean_squared_error
from math import sqrt
from azureml.core.webservice import Webservice

service = Webservice(workspace=ws, name=service_name)
predicted = service.run(input_data=test_json)
predicted = [float(i) for i in predicted]

#collecting actual values 
actuals = data_test.select('actuals').rdd.flatMap(lambda x: x).collect()
rms = sqrt(mean_squared_error(actuals, predicted))

print("RMSE Value :",rms)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 7: Retrain Model
# MAGIC 
# MAGIC In scenarios where data changes frequently, the ML models need to be frequently retrained, so that the performance continues to be acceptable (or improved).
# MAGIC 
# MAGIC In this tutorial we will cause the model to be automatically retrained and redeployed by changing the source code in the Azure Repos. Changing the source code will cause the training and deployment pipelines created earlier to be triggered.
# MAGIC 
# MAGIC We will change the code in the `train.py` file. The change is to point the model to a different data. ie. instead of using <b>'Azure_ml_train.parquet'</b> to train the model, <b>'Azure_ml_test.parquet'</b> will be used. 
# MAGIC 
# MAGIC ## Task: 7.1: Update the file `train.py` in the Azure Repo.
# MAGIC 
# MAGIC Change the code as follows:
# MAGIC 
# MAGIC 1) Comment the line:      <b> train_data =  "Azure_ml_train.parquet" </b>
# MAGIC 
# MAGIC 2) Uncomment this line:   <b> train_data   = "Azure_ml_test.parquet" </b>
# MAGIC 
# MAGIC .![azure machine learning](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-10-retrain.png)
# MAGIC 
# MAGIC ## Task 7.2: Check that the pipelines completed succesfully.
# MAGIC 
# MAGIC Note that the retrained model is redeployed model at the same web service end-point. If desired, it is possible to create a new end-point. 
# MAGIC 
# MAGIC .![azure machine learning](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-36-logs.png)
# MAGIC 
# MAGIC ## Task 7.3: Verify that a new model was indeed created by checking the version number and creation time in the Azure ML workspace.
# MAGIC 
# MAGIC .![azure machine learning](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/Lab7-37-model.png)
# MAGIC 
# MAGIC </n>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Warning : 
# MAGIC Sometimes due to limited availability of ACI instances in a region, the release pipeline might fail while deploying the re-trained model. In that case, the following error messages will be seen:
# MAGIC - `Service deployment polling reached non-successfull terminal state, current service state: Failed`
# MAGIC - `ACI Deployment failed with exception: Your container application crashed.`
# MAGIC 
# MAGIC ![error](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/ACI Error.png)
# MAGIC 
# MAGIC ###### In case we run into this issue, we will have to wait for ~10 minutes after deploying the training pipeline before deploying the re-training pipeline. This will allow Azure to provision the resources required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7.4: Get the RMSE score of the retrained model and compare with the previous one

# COMMAND ----------

# DBTITLE 1,Connect to the retrained web service
from azureml.core.webservice import Webservice
retrain_service_name = "lab7acideployment" # this should be same as the one you specified while deploying to ACI in Task 6.3.3.7
retrain_service_retrain = Webservice(workspace=ws, name = retrain_service_name)
print(retrain_service_retrain.scoring_uri)

# COMMAND ----------

# DBTITLE 1,Get some prediction from the retrained model 
#prediction using updated servicesf
import json
from pyspark.sql.functions import *

data_test = test.groupBy('actuals').agg(first('duration').alias('duration'),first('features').alias('features'),first('normFeatures').alias('normFeatures')).limit(10)

test_json = json.dumps(data_test.toJSON().collect())

# serviceretrain.run(input_data=test_json)
#selecting actuals and predicted values and creating prediction dataframe

retrain_service_retrain = Webservice(workspace=ws, name=retrain_service_name)
data_retrain, data_retrain_test = test.randomSplit([0.5,0.5])
data_retrain_test.cache()
actuals = data_retrain_test.select('actuals').rdd.flatMap(lambda x: x).collect()
predicted = retrain_service_retrain.run(input_data=test_json)
predicted = [float(i) for i in predicted]
prediction_df = sqlContext.createDataFrame(zip(actuals, predicted), schema=['Actual', 'Predicted'])
display(prediction_df)

# COMMAND ----------

# DBTITLE 1,Get the RMSE score of the retrained model and compare with the previous one
from sklearn.metrics import mean_squared_error
from math import sqrt
from azureml.core.webservice import Webservice

retrain_service_retrain = Webservice(workspace=ws, name = retrain_service_name)

predicted = retrain_service_retrain.run(input_data=test_json)
predicted = [float(i) for i in predicted]

#collecting actual values 
actuals = data_test.select('actuals').rdd.flatMap(lambda x: x).collect()
rmsvalue = sqrt(mean_squared_error(actuals, predicted))
  
print("RMSE Value :",rmsvalue)

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 8 : Delete the Azure Container Instance
# MAGIC 
# MAGIC Unless you explictly delete it, the Azure Container Instance you created in step 6.3.3.7 will continue to run and you will accrue Azure charge. You can delete it as described below. 
# MAGIC 
# MAGIC The name of the service will be the one you gave while creating the release pipeline in Task 6.3.3.7  
# MAGIC <br>
# MAGIC 1. Go to your `Resource group`
# MAGIC 2. Select `Azure Container Service`.
# MAGIC 3. Click `Delete`.
# MAGIC 
# MAGIC ![DeleteACI](https://nytaxidataset.blob.core.windows.net/notebookimages/notebookimages/Lab7/deleteaci1.PNG)

# COMMAND ----------

################################################################### END OF MLOPS LAB ##################################################################
