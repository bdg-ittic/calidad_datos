# Databricks notebook source
df = spark.read.parquet('/mnt/adlcalidaddatos/goldzone/CalidadDatos/Reporte1.parquet')
df.groupBy('Fecha').count().show()

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

df2 = df.withColumn("Fecha", when((df.Fecha >= '2022-07-18') & (df.Fecha < '2022-08-28'), '2022-07-18')
                            .when((df.Fecha > '2022-05-31') & (df.Fecha < '2022-07-18'), '2022-07-15')
                            .when((df.Fecha >= '2022-08-29') , '2022-08-27')                    
                             .otherwise(df.Fecha))
# df2.display()

# COMMAND ----------

df2.groupBy('Fecha').count().show()

# COMMAND ----------

df2.write.mode('overwrite').format('parquet').save('/mnt/adlcalidaddatos/goldzone/CalidadDatos/Reporte1.parquet')

# COMMAND ----------

df3 = df2.toPandas()
df3.to_csv('/dbfs/mnt/adlcalidaddatos/goldzone/CalidadDatos2/Reporte.csv', index=False, header=True, sep=';',decimal=',')

# COMMAND ----------

from pyspark.sql.functions import col, asc,desc

df3 = df2.filter(df2.Fecha == '2022-07-18')
df3.groupBy('Temporal').count().orderBy(col('count').desc()).display()

# COMMAND ----------



# COMMAND ----------

df2.filter(df2.Temporal == 'sgvh-pcgs').display()

# COMMAND ----------

 spark.read.parquet('/mnt/adlcalidaddatos/goldzone/CalidadDatos/auditoria.parquet').distinct().count()

