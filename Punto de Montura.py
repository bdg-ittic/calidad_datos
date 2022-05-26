# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://goldzone@adlcalidaddatos.blob.core.windows.net",
  mount_point = "/mnt/adlcalidaddatos/goldzone",
  extra_configs = {"fs.azure.account.key.adlcalidaddatos.blob.core.windows.net":
                    dbutils.secrets.get(scope = "scope-kv-calidaddatos-dev",
                                        key = "token-adlcalidaddatos")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adlcalidaddatos/rawzonef 

# COMMAND ----------

dbutils.fs.unmount("/mnt/adlcalidaddatos")
