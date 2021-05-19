# Databricks notebook source
configs = {
  "dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
  "dfs.adls.oauth2.client.id": dbutils.secrets.get(scope="azure_datalake_scope",key="datalake_gen1_clientid"),
  "dfs.adls.oauth2.credential": dbutils.secrets.get(scope="azure_datalake_scope",key="datalake_gen1_secret"),
  "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get(scope="azure_datalake_scope",key="datalake_gen1_tenant_id"),)
}

dbutils.fs.mount(
  source = "adl://datalake165.azuredatalakestore.net",
  mount_point = "/mnt/datalake",
  extra_configs = configs
)

# COMMAND ----------

configs = {
  "fs.azure.account.key.storageaccount1605.blob.core.windows.net": dbutils.secrets.get(scope="azure_storage_account",key="access_key")
}

dbutils.fs.mount(
  source = "wasbs://taxisource@storageaccount1605.blob.core.windows.net/",
  mount_point = "/mnt/storage",
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %md ##Testing mounts

# COMMAND ----------

dbutils.fs.ls("/mnt/datalake/")

# COMMAND ----------

dbutils.fs.ls("/mnt/storage/")

# COMMAND ----------



# COMMAND ----------

