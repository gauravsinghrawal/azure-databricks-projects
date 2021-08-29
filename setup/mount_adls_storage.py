# Databricks notebook source
dbutils.secrets.list("databricks-secret-scope")

# COMMAND ----------

storage_account_name="storagegen2databricks"
client_id=dbutils.secrets.get(scope="databricks-secret-scope",key="databricks-clientid")
tenant_id=dbutils.secrets.get(scope="databricks-secret-scope",key="databricks-tenantid")
client_secret=dbutils.secrets.get(scope="databricks-secret-scope",key="databricks-secretvalue")

# COMMAND ----------

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/storagegen2databricks/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/storagegen2databricks/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/storagegen2databricks/presentation")

# COMMAND ----------


