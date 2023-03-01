# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from query import *
#from create import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
import re
import jpype
import jaydebeapi
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
sys.path.append('/var/opt/tel_spark')
from messages import *
from functions import *
import math
import numpy as np

timestart = datetime.now()

#------------------------------------------------------------------------ vStp00 --------------------------------------------------------------------------------#
print(lne_dvs())
vStp00 ='Paso [0.1]: Iniciacion de variables y parametros => Registros en variables PySaprk...'
print(etq_info(vStp00))
print(lne_dvs())

parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive1', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive1', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive2', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive2', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive3', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive3', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive4', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive4', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive5', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive5', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive6', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive6', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive7', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive7', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive8', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive8', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive9', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive9', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive10', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive10', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive11', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive11', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vtdbhive12', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vttblhive12', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_inicio_mes', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_inicio_proceso', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfec_fin_mes', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_fin_mes_act', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_ini_mes_ant', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_ini_mes_ant1', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_ini_mes_ant2', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vfecha_ini_mes_ant3', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vesquematmp', required=True, type=str, help='Entidad del proceso')
parser.add_argument('--vesquemarpt', required=True, type=str, help='Entidad del proceso')

parametros = parser.parse_args()

ventidad=parametros.ventidad
vFecha_inicio_mes=parametros.vfecha_inicio_mes
vFecha_inicio_proceso=parametros.vfecha_inicio_proceso
vFecha_fin_mes=parametros.vfec_fin_mes
vFecha_fin_mes_act=parametros.vfecha_fin_mes_act
vFecha_fin_mes_ant=parametros.vfecha_ini_mes_ant
vFecha_fin_mes_ant1=parametros.vfecha_ini_mes_ant1
vFecha_fin_mes_ant2=parametros.vfecha_ini_mes_ant2
vFecha_fin_mes_ant3=parametros.vfecha_ini_mes_ant3
vEsquematmp=parametros.vesquematmp
vEsquemarpt=parametros.vesquemarpt

args = parser.parse_args()
dict_args = vars(args)
ddb = [] 
tab = []
idx = []
print(etq_info("Parametros del Proceso....."))
print(lne_dvs())

for arg_name,  arg_value in dict_args.items():
	if str(arg_name)[8:].isdigit() and str(arg_name)[0:8] =='vtdbhive' : 
		ddb.append(arg_value)

for arg_name,  arg_value in dict_args.items():
	if str(arg_name)[9:].isdigit() and str(arg_name)[0:9] =='vttblhive' :
		i = int(str(arg_name)[9:]) 
		idx.append(i)
		if i >= 0 and i <= 8 : 
			tab.append(arg_value+vEsquematmp)
		else :
			tab.append(arg_value+vEsquemarpt)
	print(etq_info(log_p_parametros(arg_name,arg_value)))

vTablas = convert(idx,ddb,tab)
vTablas.sort(key = lambda x: x[0])   #index 0 se refiere al primer elemento de la lista


#------------------------------------------------------------------------ vStp0 --------------------------------------------------------------------------------#

vStp00 ='Paso [0.2]: Inicio de sesion en Spark Session...'
print(etq_info(vStp00))
print(lne_dvs())

ts_step = datetime.now()

spark = SparkSession\
    .builder\
    .config("spark.sql.broadcastTimeout", "36000") \
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

#print(etq_info("INFO: Mostrar aplication_id => {}".format(str(app_id))))
#print(lne_dvs())

te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp00,vle_duracion(ts_step,te_step))))
print(lne_dvs())

for vPaso , vTdbhive, vTtblhive in vTablas :
	if vPaso >= 1  and vPaso <= 12   :
		vStp ='Paso ['+str(vPaso)+']: Llenado de la Tabla : '+ vTtblhive 
		print(etq_info(vStp))
		print(lne_dvs())
		ts_step = datetime.now()
		try:
			vStp ='Paso ['+str(vPaso)+'.0]: Proceso de creacion y carga de la tabla temporal...' +vTdbhive+"."+vTtblhive
			print(etq_info(vStp))
			print(lne_dvs())
			if vPaso >= 1 and vPaso <= 8 :
				vStp ='Paso ['+str(vPaso)+'.1]: Drop de la tabla...' +vTdbhive+"."+vTtblhive
				print(etq_info(vStp))
				print(lne_dvs())
				vSQL = drop_table (vPaso - 1,vTablas )
				print(etq_info("Borrado de la tabla temporal ..."+vSQL))
				spark.sql(vSQL)
				print(lne_dvs())
			else :
				vStp ='Paso ['+str(vPaso)+'.1]: Borrado de la partición de la tabla...' +vTdbhive+"."+vTtblhive
				print(etq_info(vStp))
				print(lne_dvs())
				vSQL = alter_partition(vPaso - 1,vTablas,vFecha_inicio_mes)
				print(etq_info("Query de las tablas HIVE a considerar ..."+vSQL))
				spark.sql(vSQL)
				print(lne_dvs())
	
			vSQL = exp_function(vPaso - 1,vTablas,vFecha_inicio_proceso,vFecha_fin_mes,vFecha_fin_mes_act,vFecha_inicio_mes,vFecha_fin_mes_ant,\
			vFecha_fin_mes_ant1,vFecha_fin_mes_ant2,vFecha_fin_mes_ant3 )
			print(etq_info("Query de las tablas HIVE a considerar ..."+vSQL))    
			print(lne_dvs())
			spark.sql(vSQL)
			vSQL = "Select count(*) From "+vTdbhive +"."+ vTtblhive
			df = spark.sql(vSQL)
			pdf = df.toPandas()
			VHive = pdf.iloc[0, 0]
			print(etq_info(msg_t_total_registros_hive("Total registros insertados en la tabla: ",str(VHive ))))
			print(lne_dvs())
		except Exception as e:
			exit(etq_error(msg_e_ejecucion(vTdbhive+"."+ vTtblhive+" Grabando los datos =>".format(''),str(e))))

		te_step = datetime.now()
		print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
		print(lne_dvs())

timeend = datetime.now()
spark.stop()
print(etq_info(msg_d_duracion_ejecucion(" DURACION FINAL DE TODO EL PROCESO DE LA ENTIDAD : " + ventidad,vle_duracion(timestart,timeend ))))
print(lne_dvs())




