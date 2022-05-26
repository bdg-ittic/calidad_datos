# Databricks notebook source
# MAGIC %md
# MAGIC # Funciones para determinar los scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completitud

# COMMAND ----------

def completitud(df_dataset, size):
    """
      Funcion para hallar el puntaje del criterio de completitud, encontrado el número total de nulos del dataset, divido por el número total del registros del dataset, esta operación da un resultado entre 0-1, por tal razón se multiplica por 10 para dar como resultado entre 0 y 10
      Parametros:
        df_dataset: DataFrame spark
        size: tamaño del conjunto de datos  
      Return:
        score_cpl: Valor del puntaje del criterio de calidad, valor entre 0 y 10.
     """
    
    if size < 50 :
        score_cpl = 0
        

    else:
        estadoNulos = df_dataset.select([count( when(col(cols).isNull(), cols) ).alias(cols) for cols in df_dataset.columns])
        totalNulos = estadoNulos.withColumn('totalNulos', sum(estadoNulos[col] for col in estadoNulos.columns))

        conteoFilasXCols = size * len(df_dataset.columns)
        totalNulos = totalNulos.first()['totalNulos']
        porcentajeNulos = (10 * totalNulos) / conteoFilasXCols

        score_cpl = 10 - int(porcentajeNulos)

    return score_cpl


# COMMAND ----------

# MAGIC %md
# MAGIC ## Actualidad

# COMMAND ----------

def actualidad(fec_last_modified, frecuency, now):
    """
    Des:
    Función para hallar el puntaje del criterio de actualidad, donde se halla la diferencia en días entre la fecha actual y la última fecha de actualización del dataset, este resultado se compara con la frecuencia de actualización del dataset, donde si la diferencia es mayor a la frecuencia de actualización el criterio da como resultado 0 y quiere decir que el conjunto de datos está desactualizado, de lo conrrario será 10 lo que quiere decir que el conjunto de datos está actualizado
    Params:
    fec_last_modified: fecha de la utlima modificación del dataset, dato que llega de la metadata
    frecuency: Frecuencia de actualización del conjunto de datos, puede ser anual, trienio, semestral, etc..
    now: Fecha actual del sistema
    return:
    score_act: Puntaje del criterio de actualidad, valor que puede ser 0 o 10 dependiendo la condición
    """
  
  #Diferencia entre fechas
    dif_date = now - fec_last_modified
    dif_date = int(dif_date.days)

    frecuency = frecuency.lower()

    #Diccionario para determinar valor numero para la frecuencia de actualización
    frecuency_dic = {'anual' : 365, 'trienio' : 1095, 'semestral' : 180, 'trimestral' : 90, 'semanal' : 8, 'mensual': 30}

    try:
        frecuency = frecuency_dic[frecuency]
    except Exception as e:
        frecuency = 365


    # Puntaje de actualidad
    if dif_date > frecuency:
        score_act = 0

    else:
        score_act = 10

    return score_act


# COMMAND ----------

# MAGIC %md
# MAGIC ## Trazabilidad

# COMMAND ----------

def trazabilidad(metadata):
    """
    Función para hallar el puntaje del criterio de trazabilidad, se espera 40 datos en la metada, dependiendo la cantidad de datos que lleguen en la metada se da puntaje al criterio, por cada 4 datso faltante la calificación disminuye un punto, es decir si llegaron 36 datos en la metadata, el puntaje será 9, si llegan 32 será 8 y asi sucesivamente.
    Params:
    Metada: diccionario, con los campos de la metadata
    return:
    score_traz: Valor del criterio, va entre 0 y 10
    """

    score_traz = int(round(sin_nulos/61 * 10,0))
    return score_traz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidad

# COMMAND ----------

def unicidad(df_dataset):
    """
    Función que da puntaje al criterio de Unicidad, cuenta las filas repetidas en el conjunto de datos
    Params:
    df_dataset: COnjunto de datos a evaluar
    return:
    score_uni: PUntaje del criterio, con valor entre 0 a 10 dependiedo de las filas repetidas
    """
  
    try:
        col_list = df_dataset.columns
        df_dataset = df_dataset.withColumn('concatCols', concat_ws('_', *col_list))

        ConteoFilas = df_dataset.count()

        conteoRepetidos =  df_dataset.groupBy('concatCols').count()
        totalRepetidos = conteoRepetidos.filter(conteoRepetidos['count'] > 1).groupBy().sum('count').first()["sum(count)"]

        if totalRepetidos == None:
            totalRepetidos = 0

        score_uni = 10 - int((totalRepetidos/ConteoFilas)*10)
    except:
        score_uni = 10
    return score_uni

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disponibilidad

# COMMAND ----------

def disponibilidad(score_act):
    """
    Función de da el puntaje de la disponibilidad. Promedio entre el puntaje de actualidad, y 10, dado que siempre se están evaluando datasets publicos, si se establece data set privados, seria el promedio entre actualidad y 0
    Params:
    score_act: puntaje del criterio de actualidad
    return:
    score_disp: Puntaje del criterio, valor que va entre 5 a 10
    """
    score_disp = int((score_act + 10)/2)
    return score_disp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portabilidad

# COMMAND ----------

def portabilidad(score_cpl, score_conf):
    """
    Función del puntaje de portabilidad, que es el promedio entre los criterios de completitud y el de confomidad
    Params:
    score_cpl: Puntaje del criterio de completutid
    score_conf: Putaje del criterio de conformidad
    return:
    score_port: Puntaje del critero, que va entre 0 10
    """
    score_port = int((score_cpl + score_conf)/2)
    return score_port
