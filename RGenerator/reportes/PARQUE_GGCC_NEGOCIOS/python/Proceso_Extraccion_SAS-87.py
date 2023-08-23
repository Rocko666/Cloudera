# Cruce de Informacion Grandes Cuentas
# Realizado: Luis Fernando Llanganate
# FEcha: 10-10-2022
## Basic Libraries
import sys
import re
import pyspark
import os.path
import shutil
import numpy as np
import pandas as pd
import argparse
from os import path
from datetime import datetime, timedelta

## Spark libraries
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, to_timestamp, lag, lead, first, last, rank, unix_timestamp, concat, lit, expr
from pyspark.sql.functions import to_date, date_format
from pyspark.sql import SQLContext, SparkSession, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import QuantileDiscretizer, Bucketizer
from pyspark.sql.types import StringType

reload(sys)
sys.setdefaultencoding('utf-8')
#pip install xlrd==1.2.0

parser = argparse.ArgumentParser()
parser.add_argument('--ruta', required=True, type=str)
parser.add_argument('--fechafin', required=True, type=str)
parser.add_argument('--tabla1', required=True, type=str)
parser.add_argument('--tabla2', required=True, type=str)
parser.add_argument('--tabla3', required=True, type=str)
parser.add_argument('--queue', required=True, type=str)

parametros = parser.parse_args()
vRuta=parametros.ruta
vFechafin=parametros.fechafin
VTablaT1=parametros.tabla1
VTablaT2=parametros.tabla2
VTablaT3=parametros.tabla3
VQueue=parametros.queue

print(VTablaT1)
print(VTablaT2)
print(VTablaT3)
print(vRuta)
print(vFechafin)
print(VQueue)

spark = SparkSession\
	.builder\
	.enableHiveSupport() \
	.config("spark.sql.broadcastTimeout", "36000") \
	.config("hive.exec.dynamic.partition", "true") \
	.config("hive.exec.dynamic.partition.mode", "nonstrict") \
	.config("spark.yarn.queue", 'default') \
	.config("hive.enforce.bucketing", "false") \
	.config("hive.enforce.sorting", "false") \
    .config("spark.driver.maxResultSize", "30g") \
	.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print("INFO: Mostrar application_id => {}".format(str(app_id)))

sqlContext = SQLContext(spark)
hc = HiveContext(spark)   

o_stdout = sys.stdout

query = "Select Distinct fechaproceso as filas  From " + VTablaT2 + " Where fechaproceso = " + vFechafin 
print(query)
df=spark.sql(query)
if df.count() == 0 :
	print(" La ultima fecha del mes no existe : " + vFechafin + " favor carguela primero")  
        sqlContext.clearCache()
        sc.stop()
        exit(1)
print("Ejecutando proceso principal") 
try:
	query = "SELECT T1.fecha_parque AS FECHA_PARQUE, concat('"+chr(92)+chr(39)+"',T1.cedula) AS CEDULA, concat('"+chr(92)+chr(39)+"',T1.ruc) AS RUC, T1.account_no AS ACCOUNT_NO,"
	query = query + "T1.nombre AS NOMBRE, T1.apellido AS APELLIDO, T1.razon_social AS COMPANIA, T1.cod_categoria AS COD_CATEGORIA," 
	query = query + "T1.cod_plan_activo AS COD_PLAN_ACTIVO,T1.plan AS PLAN, T1.segmento AS SEGMENTO, T1.subsegmento AS SUBSEGMENTO,"
	query = query + "T1.provincia AS PROVINCIA, T1.canton AS CANTON, T1.parroquia AS PARROQUIA, T1.tipo_movimiento AS TIPO_MOVIMIENTO," 
	query = query + "T2.AREA AS AREA, T2.codigo_vendedor_da AS CODIGO_VENDEDOR_DA, T2.jefatura AS JEFATURA, T2.ejecutivo_asignado AS EJECUTIVO_ASIGNADO,"
	query = query + "T2.region AS REGION, T2.tipopersona AS TIPO_PERSONA, T3.categoria  AS TIPO, Count(T1.telefono) AS PARQUE "
	query = query + "FROM " + VTablaT1 + " AS T1 "
	query = query +" LEFT JOIN ( Select * From ( SELECT ROW_NUMBER() OVER ( PARTITION BY identificador,fecha_ingreso  ORDER BY identificador) as d, * FROM " +  VTablaT2 + " Where  fechaproceso = "+ vFechafin + ") X  Where d = 1 ) as T2 ON  T2.identificador  =  T1.ruc "
	query = query +" JOIN "+ VTablaT3 + " AS T3 ON T1.cod_plan_activo  = T3.cod_plan_activo "
	query = query + "WHERE T1.parque = 'CONTRATO' AND "
	query = query + "T1.fecha_proceso = "+vFechafin+" AND T1.linea_negocio IN ('POSPAGO', 'POSTPAGO') AND T1.segmento IN ('PYMES', 'GRANDES CUENTAS') "
	query = query + "GROUP BY T1.fecha_parque, T1.cedula,T1.ruc, T1.account_no, T1.nombre, T1.apellido, T1.razon_social, T1.cod_categoria, T1.cod_plan_activo, "
	query = query + "T1.plan, T1.segmento, T1.subsegmento, T1.provincia, T1.canton, T1.parroquia, T1.tipo_movimiento, T1.parque, T1.fecha_proceso, "
	query = query + "T1.linea_negocio, T2.area, T2.codigo_vendedor_da, T2.jefatura, T2.ejecutivo_asignado, T2.region, T2.tipopersona, T3.categoria "

	print(query)
        df_extrctr = spark.sql(query)
        df_extrctr.printSchema()

        pandas_df = df_extrctr.toPandas()
        pandas_df.to_csv(vRuta,  header = True, index = False, sep='|', encoding='UTF-8')
        pandas_df['RUC'] =pandas_df.RUC.str.slice(1)
        pandas_df['CEDULA'] =pandas_df.CEDULA.str.slice(1)

	#transformacion del archivo csv a excel
        pandas_df.to_excel (vRuta,index = None,sheet_name='PRQGGCCNEG')
except Exception as e:
        print("Error PySpark: \n %s" % e)
finally:
        print("============CRUCE DE INFORMACION REALIZADO CON EXITO=======" + vFechafin ) 
        sqlContext.clearCache()
        sc.stop()

