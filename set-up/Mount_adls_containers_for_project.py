# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess to azure data lake using Service Principal
# MAGIC
# MAGIC 1. Get client id, tenant id, client secret from keyvault
# MAGIC 2. Set Spark config with App/Client id,Directory Tenant id & secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC

# COMMAND ----------

def mount_dlfs(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope= 'formula1-scope',key= 'client-id' )
    tenant_id = dbutils.secrets.get(scope= 'formula1-scope',key= 'tenant-id' )
    client_secret = dbutils.secrets.get(scope= 'formula1-scope',key= 'client-secret' )

    configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
        
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_dlfs('formula1dlud','processed')

# COMMAND ----------

mount_dlfs('formula1dlud','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())
