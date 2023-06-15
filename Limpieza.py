# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType
from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC # Limpieza de auditoria

# COMMAND ----------

schema_aud = StructType([ \
    StructField("uid",StringType(),True), \
    StructField("errores",StringType(),True)  
  ])

df_log_errores = spark.createDataFrame([], schema_aud)   

# COMMAND ----------

data2 = [('inicio', 'procesado')]
df_log = spark.createDataFrame(data=data2, schema= schema_aud)
df_log.write.mode('overwrite').format('parquet').save(f'mnt/adlcalidaddatos/goldzone/CalidadDatos/auditoria.parquet')    


# COMMAND ----------

# MAGIC %md
# MAGIC # Fecha Inicio

# COMMAND ----------

fecha_actual = datetime.today().strftime('%Y-%m-%d')
dbutils.notebook.exit(fecha_actual)

# COMMAND ----------

 df_log.write.mode('overwrite').format('parquet').save(f'mnt/adlcalidaddatos/goldzone/CalidadDatos/log.parquet')  
