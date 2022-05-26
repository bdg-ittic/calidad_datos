# Databricks notebook source
# MAGIC %md
# MAGIC # Ingetar Portal datos abiertos

# COMMAND ----------

# MAGIC %md 
# MAGIC # Librerias

# COMMAND ----------

# MAGIC %pip
# MAGIC !pip install sodapy

# COMMAND ----------

import requests
from sodapy import Socrata
import pandas as pd
import os, sys
from pyspark.sql.functions import isnan, when, count, col, concat_ws, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Parametros

# COMMAND ----------

fecha_actual = datetime.today().strftime('%Y-%m-%d')
fecha_actual = datetime.strptime(fecha_actual, "%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC # Estructura reporte final

# COMMAND ----------

schema = StructType([
  StructField('Temporal', StringType(), True),
   StructField('Entidad', StringType(), True),
  StructField('Categoria', StringType(), True),
  StructField('Conjunto_Datos', StringType(), True),
  StructField('Registros', StringType(), True),
  StructField('Fecha_actualización', DateType(), True),



  StructField('Completitud', IntegerType(), True),
  StructField('Credibilidad', IntegerType(), True),
  StructField('Actualidad', IntegerType(), True),
  StructField('Trazabilidad', IntegerType(), True),
  StructField('Disponibilidad', IntegerType(), True),
  StructField('Conformidad', IntegerType(), True),
  StructField('Comprensibilidad', IntegerType(), True),
  StructField('Portabilidad', IntegerType(), True),
  StructField('Consistencia', IntegerType(), True),
  StructField('Exactitud', IntegerType(), True),
  StructField('Unicidad', IntegerType(), True)
  ])

df_calidad_datos = spark.createDataFrame([], schema)   

# COMMAND ----------

# MAGIC %md
# MAGIC # Importar funciones

# COMMAND ----------

# MAGIC %run /Repos/CalidadDatos/calidad_datos/criterios

# COMMAND ----------

# MAGIC %md
# MAGIC # Metadata e Inventario

# COMMAND ----------

client = Socrata("www.datos.gov.co", None)

client = Socrata("www.datos.gov.co",
                 "vEcNldBkhGB4NuxuknAbVjQjV",
                 username="datosabiertos@mintic.gov.co",
                 password="Datos$2022")

results = client.get("uzcf-b9dh", limit = "100000")

# COMMAND ----------

# MAGIC %md
# MAGIC # Listar los dataset a procesar
# MAGIC <br>
# MAGIC <ul>
# MAGIC   <li>Publicos </li>
# MAGIC   <li>aprobados </li>
# MAGIC   <li>menor a 1'000.000 </li>
# MAGIC </ul>

# COMMAND ----------

df_metadata_original = pd.DataFrame(results)
df_metadata_procesados = df_metadata_original[(df_metadata_original.audience == 'public') & (df_metadata_original.approval_status == 'approved') & (df_metadata_original.type == 'dataset') & (df_metadata_original.row_count.astype('float') < 1000000)]

# Aprobado status, publicos audince, tipo data set type, row_count menor a 1 millon
print("archivos leidos", len(df_metadata_original))
dataset_len =  len(df_metadata_procesados)
print("archivos procesados", dataset_len)

# COMMAND ----------

df_metadata_procesados.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Petición y Calificación Dataset

# COMMAND ----------

inicio = 1
contador = inicio
for row in df_metadata_procesados.index[inicio:50]:

    #################################################### Ingesta #######################################################################################
    
    # Parametro para realizar la petición
    dataset = df_metadata_procesados['uid'][row]                   # Identificador del dataset
    rows = int(df_metadata_procesados['row_count'][row])           # Número de filas del dataset
    offset = 0                                                     # Offset inicial 

    # Datos de control de carga
    print("dataset: ", dataset, "record:", rows)
    print(contador, "de", dataset_len)

    #Petición al api para recrear dataset
    request_datset = requests.get(f'https://www.datos.gov.co/resource/{dataset}?$limit=1000&$offset={offset}')
    request_datset = eval(request_datset.text)
    df_dataset = spark.createDataFrame(request_datset)            # dataFrame que contiene la información del dataset del portal de datos abiertos
    
    
    #Validar si el dataset tiene más de 50000 registros
#     if rows > 50000:
#         iteraciones = rows//50000 + 1
#         for i in range(1,iteraciones):
#             offset = i*50000
#             request_datset = requests.get(f'https://www.datos.gov.co/resource/{dataset}?$limit=50000&$offset={offset}')
#             request_datset = eval(request_datset.text)
#             df_datset_temp = pd.DataFrame(request_datset)
#             df_dataset = pd.concat([df_dataset, df_datset_temp]) 


    ################################ Calificación de los criterios de calidad ############################################################
    
    #Parametros 
    fecha_actualizacion = datetime.strptime(df_metadata_procesados['last_data_updated_date'][row][:10], '%Y-%m-%d')
    frecuencia_actualizacion = df_metadata_procesados['informacindedatos_frecuenciadeactualizacin'][row]
    datos_metadata = len(df_metadata_procesados.iloc[contador].dropna())
    
    #Calculo de los scores
    score_completitud = completitud(df_dataset, rows)
    score_actualidad = actualidad(fecha_actualizacion,frecuencia_actualizacion,fecha_actual)
    score_trazabilidad = trazabilidad(datos_metadata)
    score_unicidad = unicidad(df_dataset)
    score_disponibilidad = disponibilidad(score_actualidad)
    score_conformidad = 10
    score_portabilidad = portabilidad(score_completitud, score_conformidad)
    score_credibilidad = 10
    score_comp = 10
    score_consistencia = 10
    score_exactitud = 10
    
    ########################################## Generar reporte ######################################################################################
    
    #Parametros para el reporte
    name = df_metadata_procesados['name'][row] 
    entidad = df_metadata_procesados['owner'][row] 
    categoria = df_metadata_procesados['category'][row] 
    name_dataset = df_metadata_procesados['name'][row] 
    
    vals = [(dataset,
               entidad, 
               categoria, 
               name, 
               rows, 
               fecha_actualizacion, 
               score_completitud, 
               score_credibilidad, 
               score_actualidad, 
               score_trazabilidad, 
               score_disponibilidad, 
               score_conformidad, 
               score_comp, 
               score_portabilidad, 
               score_consistencia,  
               score_exactitud, 
               score_unicidad)]
     
    df_temp = spark.createDataFrame(vals, schema)
    
    df_calidad_datos = df_calidad_datos.union(df_temp)
    
    contador += 1
    print("-----------------------------------------------------------------------")

# COMMAND ----------

df_calidad_datos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Escribir reporte en ADL

# COMMAND ----------

df_calidad_datos.write.mode('overwrite').format('parquet').save('mnt/adlcalidaddatos/goldzone/CalidadDatos/Reporte.parquet')

# COMMAND ----------

file_path = dbutils.fs.ls('mnt/adlcalidaddatos/goldzone/CalidadDatos/Reporte.parquet')  
#Se almacena en la variable ruta unicamenta la ruta del archivo .csv
for file in range(0,len(file_path)):
    if file_path[file][1].endswith(".parquet")==True:
        ruta=file_path[file]
    else:
        ruta=''

ruta = ruta.path
#Comando para copiar el archivo fuera de la carpeta creada por defecto a la ruta del dataset
dbutils.fs.cp(ruta,'mnt/adlcalidaddatos/goldzone/CalidadDatos2/Reporte.csv')
# #Comando para eliminar los archivos de transaccion
# dbutils.fs.rm(f'{path_cur}{name_file}',recurse=True)
