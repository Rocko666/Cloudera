import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import argparse
sys.path.append('/var/opt/tel_spark')
from messages import *
from functions import *
from query import *


timestart = datetime.now()

#------------------------------------------------------------------------ vStp00 --------------------------------------------------------------------------------#
print(lne_dvs())
vStp00 ='Paso [0]: Iniciacion de variables y parametros => Registros en variables PySaprk...'
print(etq_info(vStp00))
print(lne_dvs())

parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=True, type=str, help='Entidad del Proceso')
parser.add_argument('--vclass', required=True, type=str, help='Clase java del conector JDBC')
parser.add_argument('--vjdbcurl', required=True, type=str, help='URL del conector JDBC Ejm: jdbc:mysql://localhost:3306/base')
parser.add_argument('--vusuariobd', required=True, type=str, help='Usuario de la base de datos')
parser.add_argument('--vclavebd', required=True, type=str, help='Clave de la base de datos')
parser.add_argument('--vhivebd1', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vtablahive1', required=True, type=str, help='Nombre de la tabla en hive bd.tabla')
parser.add_argument('--vhivebd2', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vtablahive2', required=True, type=str, help='Nombre de la tabla en hive bd.tabla')
parser.add_argument('--vtipocarga', required=True, type=str, help='Tipo de carga overwrite/append - carga completa/incremental')
parser.add_argument('--vfechainicio', required=True, type=str, help='Fecha de inicio del proceso')
parser.add_argument('--vcolumnapt', required=True, type=str,help='Parametro de columna particion')
parser.add_argument('--vtabladef', required=True, type=str,help='Parametro de tabla definitiva')
parser.add_argument('--vfechafin', required=True, type=str,help='Parametro fecha final del traslado de datos')
parser.add_argument('--vtdboracle', required=True, type=str,help='Parametro fecha final del traslado de datos')
parser.add_argument('--vttbloracle', required=True, type=str,help='Parametro fecha final del traslado de datos')


parametros = parser.parse_args()
ventidad=parametros.ventidad
vClass=parametros.vclass
vUrlJdbc=parametros.vjdbcurl
vUsuarioBD=parametros.vusuariobd
vClaveBD=parametros.vclavebd
vBaseHive1=parametros.vhivebd1
vTablaHive1=parametros.vtablahive1
vBaseHive2=parametros.vhivebd2
vTablaHive2=parametros.vtablahive2
vTipoCarga=parametros.vtipocarga
vFechainicio=parametros.vfechainicio
vColumnapt=parametros.vcolumnapt
vTabladef=parametros.vtabladef
vFechafin=parametros.vfechafin
vTdboracle=parametros.vtdboracle
vTtbloracle=parametros.vttbloracle

print(etq_info("Parametros del Proceso....."))
print(lne_dvs())

print(etq_info(log_p_parametros("ventidad",ventidad)))
print(etq_info(log_p_parametros("vClass",vClass)))
print(etq_info(log_p_parametros("vUrlJdbc",vUrlJdbc)))
print(etq_info(log_p_parametros("vUsuarioBD",vUsuarioBD)))
print(etq_info(log_p_parametros("vClaveBD",vClaveBD)))
print(etq_info(log_p_parametros("vtipocarga",vTipoCarga)))
print(etq_info(log_p_parametros("vBaseHive1",vBaseHive1)))
print(etq_info(log_p_parametros("vTablaHive1",vTablaHive1)))
print(etq_info(log_p_parametros("vBaseHive2",vBaseHive2)))
print(etq_info(log_p_parametros("vTablaHive2",vTablaHive2)))
print(etq_info(log_p_parametros("vTipoCarga",vTipoCarga)))
print(etq_info(log_p_parametros("vFechainicio",vFechainicio)))
print(etq_info(log_p_parametros("vColumnapt",vColumnapt)))
print(etq_info(log_p_parametros("vTabladef",vTabladef)))
print(etq_info(log_p_parametros("vFechafin",vFechafin)))
print(etq_info(log_p_parametros("vTdboracle",vTdboracle)))
print(etq_info(log_p_parametros("vTtbloracle",vTtbloracle)))

print(lne_dvs())

#------------------------------------------------------------------------ vStp01 --------------------------------------------------------------------------------#

vStp01 ='Paso [1]: Inicio de sesion en Spark Session...'
print(etq_info(vStp01))
print(lne_dvs())

ts_step = datetime.now()

spark = SparkSession. \
    builder. \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    enableHiveSupport(). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId

print(etq_info("INFO: Mostrar aplication_id => {}".format(str(app_id))))
print(lne_dvs())

te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp01,vle_duracion(ts_step,te_step))))
print(lne_dvs())

#------------------------------------------------------------------------ vStp02 --------------------------------------------------------------------------------#
vStp02 ='Paso [2.1]: Creacion de Dataframe con lectura desde ORACLE...'
print(etq_info(vStp02))
print(lne_dvs())

ts_step = datetime.now()

try:
    print(etq_info("Recuperando datos de la tabla de Oracle ..."+vTdboracle+"."+ vTtbloracle))
    print(lne_dvs())
    vSQL = export_oracle_otc_v_sim_trx_5d(vTdboracle,vTtbloracle,vFechainicio)
    print(etq_info("Query de la tabla de Oracle a exportar a Hive ..."+vSQL))    
    print(lne_dvs())
    df_otc_v_sim_trx_5d = spark.read.format("jdbc")\
			.option("url",vUrlJdbc)\
			.option("driver",vClass)\
			.option("user",vUsuarioBD)\
			.option("password",vClaveBD)\
			.option("fetchsize",1000)\
			.option("dbtable",vSQL).load()
    print(etq_info(msg_t_total_registros_hive("Total registros recuperados de ORACLE: ",str( df_otc_v_sim_trx_5d.count()))))
    df_otc_v_sim_trx_5d.printSchema()
    df_otc_v_sim_trx_5d.show(5)
    # Grabando en tabla temporal de Hive previo envio a ORACLE.
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
    print(lne_dvs())
    vStp02 ='Paso [2.2]: Guardando los datos leidos en ORACLE a HIVE en la Tabla Temporal...'+vBaseHive1+"."+ vTablaHive1
    print(etq_info(vStp02))
    print(lne_dvs())
    df_otc_v_sim_trx_5d.write.mode("overwrite").saveAsTable(vBaseHive1+"."+ vTablaHive1)
    ts_step = datetime.now()
    print(lne_dvs())
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vTdboracle+"."+ vTtbloracle+" Grabando los datos =>".format(''),str(e))))
te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp02,vle_duracion(ts_step,te_step))))
print(lne_dvs())

#------------------------------------------------------------------------ vStp03 --------------------------------------------------------------------------------#
vStp03 ='Paso [3]: Operaciones en HIVE en tabla definitiva...' + vBaseHive2+"."+vTablaHive2
print(etq_info(vStp03))
print(lne_dvs())

ts_step = datetime.now()

try:
    vSQL = "Select Distinct "+vColumnapt+" as filas  From " + vBaseHive2+"."+vTablaHive2+ " Where "+vColumnapt+" >= "+vFechainicio+ " and "+vColumnapt+" <= "+vFechafin
    print(etq_info("Query conteo de Datos en la tabla definitiva de HIVE ..."+vSQL))   

    df1=spark.sql(vSQL)

    data_collect = df1.collect()
    print(etq_info("Numero de particiones a borrar en la tabla definitiva : " + str(df1.count() ))) 

    if (df1.count() > 0 ) :
	for row in data_collect:
        VFechaP = row["filas"]
        print(etq_info("Borrando particion: "+str(VFechaP)))
        query_truncate = "ALTER TABLE "+vBaseHive2+"."+vTablaHive2+" DROP IF EXISTS PARTITION ( "+vColumnapt+" = "+str(VFechaP)+" ) purge"
    print(etq_info(query_truncate))
    df1=spark.sql(query_truncate)
    filtro = vColumnapt + " >= " +  vFechainicio + " and " + vColumnapt + " <= " + vFechafin
    print(etq_info("Filtro aplicado : " + filtro ))

    df_otc_v_sim_trx_5d = df_otc_v_sim_trx_5d.filter(filtro)
    print(etq_info("Numero de Registros a insertar en la tabla definitiva a dia caido: "+str(df_otc_v_sim_trx_5d.count()) ))

    df_otc_v_sim_trx_5d.repartition(1).write.mode("append").insertInto(vBaseHive2+"."+vTablaHive2)
    ts_step = datetime.now()
    print(lne_dvs())

except Exception as e:
	exit(etq_error(msg_e_ejecucion(vBaseHive2+"."+ vTablaHive2+" Grabando los datos =>".format(''),str(e))))
te_step = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vStp03,vle_duracion(ts_step,te_step))))
print(lne_dvs())

timeend = datetime.now()
spark.stop()
print(etq_info(msg_d_duracion_ejecucion(" DURACION FINAL DE TODO EL PROCESO DE LA ENTIDAD : " + ventidad,vle_duracion(timestart,timeend ))))
print(lne_dvs())