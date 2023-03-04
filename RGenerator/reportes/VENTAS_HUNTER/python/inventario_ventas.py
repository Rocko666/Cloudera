from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse
import pandas as pd
sys.path.append('/var/opt/tel_spark')
from messages import *
from functions import *


timestart = datetime.now()

#------------------------------------------------------------------------ vStp00 --------------------------------------------------------------------------------#
print(lne_dvs())
vStp00 ='Paso [0]: Iniciacion de variables y parametros => Registros en variables PySaprk...'
print(etq_info(vStp00))
print(lne_dvs())

parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=True, type=str,help='Esquema de la tabla t_geographical_locations_TMP ') 
parser.add_argument('--vDBTGL', required=True, type=str,help='Esquema de la tabla t_geographical_locations_TMP ') 
parser.add_argument('--vTBTGL', required=True, type=str,help='Tabla t_geographical_locations_TMP ') 
parser.add_argument('--vDBOVST5T2', required=True, type=str,help='Esquema de la tabla OTC_V_SIM_TRX_5D_TEM2') 
parser.add_argument('--vTBOVST5T2', required=True, type=str,help='Tabla OTC_V_SIM_TRX_5D_TEM2') 

parametros = parser.parse_args()
ventidad=parametros.ventidad
vDBTGL=parametros.vDBTGL
vTBTGL=parametros.vTBTGL
vDBOVST5T2=parametros.vDBOVST5T2
vTBOVST5T2=parametros.vTBOVST5T2

print(etq_info("Parametros del Proceso....."))
print(lne_dvs())

print(etq_info(log_p_parametros("ventidad",ventidad)))
print(etq_info(log_p_parametros("vDBTGL",vDBTGL)))
print(etq_info(log_p_parametros("vTBTGL",vTBTGL)))
print(etq_info(log_p_parametros("vDBOVST5T2",vDBOVST5T2)))
print(etq_info(log_p_parametros("vTBOVST5T2",vTBOVST5T2)))
print(lne_dvs())

#------------------------------------------------------------------------ vStp01 --------------------------------------------------------------------------------#

vStp01 ='Paso [1]: Inicio de sesion en Spark Session...'
print(etq_info(vStp01))
print(lne_dvs())

ts_step = datetime.now()
spark = SparkSession \
    .builder \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("hive.exec.max.dynamic.partitions","2000") \
    .appName("VENTAS_HUNTER") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId

print(etq_info("INFO: Mostrar aplication_id => {}".format(str(app_id))))
print(lne_dvs())

te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
print(lne_dvs())

#------------------------------------------------------------------------ vStp02 --------------------------------------------------------------------------------#
vStp02 ='Paso [2]: Creacion de Dataframe create_t_geographical_locations_tmp...'
print(etq_info(vStp02))
print(lne_dvs())

ts_step = datetime.now()

try:
    print(etq_info(" =========     Borrando Tabla Temporal : " + vDBTGL+"."+ vTBTGL))
    spark.sql(drop_tmp(vDBTGL,vTBTGL))
    print(etq_info(" =========     Creando Tabla Temporal : " + vDBTGL+"."+ vTBTGL))
    spark.sql(create_t_geographical_locations_tmp(vDBTGL,vTBTGL))
    df_t_geographical_locations_tmp = spark.sql(t_geographical_locations_TMP(vDBTGL,vTBTGL))
    df_t_geographical_locations_tmp.printSchema()
    df_t_geographical_locations_tmp.show(5)
except Exception as e:
	exit(etq_error(msg_e_ejecucion("Creando df: df_t_geographical_locations_tmp --> ".format(''),str(e))))
te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
print(lne_dvs())


timeend = datetime.now()
spark.stop()
print(etq_info(msg_d_duracion_ejecucion(" DURACION FINAL DE TODO EL PROCESO DE LA ENTIDAD : " + ventidad,vle_duracion(timestart,timeend ))))
print(lne_dvs())