# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import subprocess
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=False, type=str,help='Parametro de la entidad')
parser.add_argument('--vhivebd', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vfecha_ejec', required=True, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfecha_hoy', required=True, type=str,help='Parametro 2 de la query sql')
parser.add_argument('--vTabla1', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vTabla2', required=True, type=str,help='Parametro 4 de la query sql')
parser.add_argument('--val_cadena_jdbc', required=True, type=str,help='Parametro 5 de la query sql')
parser.add_argument('--val_user', required=True, type=str,help='Parametro 6 de la query sql')


parametros = parser.parse_args()
vEntidad=parametros.ventidad
vBaseHive=parametros.vhivebd
vfecha_hoy=parametros.vfecha_hoy
vfecha_ejec=parametros.vfecha_ejec
vTablaBajasInv=parametros.vTabla1
vTablaBajaInvTmp=parametros.vTabla2
vBeelineJDBC=parametros.val_cadena_jdbc
vBeelineUser=parametros.val_user

## STEP 3: Inicio el SparkSession
spark = SparkSession \
    .builder \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .appName(vEntidad) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId


def last_partition(con,vSql):
    partitions = con.sql(vSql).toPandas()
    partitions = partitions.replace(".*=", "", regex=True)
    partitions = partitions['partition'].astype('int')
    partitions = partitions.sort_values(ascending=True)
    partitions = partitions.iloc[-1]
    return partitions


##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    vStp01="Paso 0"
    if vfecha_hoy == vfecha_ejec:
        vfecha_proceso=str(last_partition(spark,part_otc_t_churn_sp2()))
    else:
        vfecha_proceso=str(vfecha_ejec)

    #bba
    #vfecha_proceso="20221224"
    vfecha_procesoD = datetime.strptime(vfecha_proceso, '%Y%m%d').date()
    vfecha_mes_ini = vfecha_procesoD.replace(day=1)
    vdia1_mes=vfecha_mes_ini.strftime('%Y%m%d')
    vfecha_finmesant = vfecha_mes_ini - relativedelta(days=1)
    vdiaf_mesant=vfecha_finmesant.strftime('%Y%m%d')

    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vfecha_hoy",str(vfecha_hoy))))
    print(etq_info(log_p_parametros("vfecha_ejec",str(vfecha_ejec))))
    print(etq_info(log_p_parametros("vdiaf_mesant",str(vdiaf_mesant))))
    print(etq_info(log_p_parametros("vfecha_proceso",str(vfecha_proceso))))
    print(etq_info(log_p_parametros("vdia1_mes",str(vdia1_mes))))
    print(etq_info(log_p_parametros("vTabla1",str(vTablaBajasInv))))
    print(etq_info(log_p_parametros("vTabla2",str(vTablaBajaInvTmp))))
    
    print(lne_dvs())
    print(lne_dvs())
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [otc_t_bajas_involuntarias]"))
    print(lne_dvs())
    df_otc_t_bajas_involuntarias=spark.sql(otc_t_bajas_involuntarias(vfecha_hoy,vdiaf_mesant,vfecha_proceso)).cache()
    df_otc_t_bajas_involuntarias.printSchema()
    ts_step_tbl = datetime.now()
    columns = spark.table(vTablaBajasInv).columns
    cols = []
    for column in columns:
        cols.append(column)                
    df_otc_t_bajas_involuntarias = df_otc_t_bajas_involuntarias.select(cols)
    query_truncate = "ALTER TABLE "+vTablaBajasInv+" DROP IF EXISTS PARTITION (proces_date = "+str(vfecha_proceso)+") purge"
    spark.sql(query_truncate)
    print(etq_info(query_truncate))    
    df_otc_t_bajas_involuntarias.repartition(1).write.mode("append").insertInto(vTablaBajasInv)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaBajasInv))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_bajas_involuntarias",str(df_otc_t_bajas_involuntarias.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_bajas_involuntarias",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df_otc_t_bajas_involuntarias   
    
        
    vStp01="Paso 2"
    print(lne_dvs())
    print(etq_info("Paso [2]: Ejecucion de funcion [del_otc_t_bajas_involuntarias]- Eliminando datos de bajas involuntarias"))
    print(lne_dvs())
    df_otc_t_bajas_involuntarias_tot = spark.sql(otc_t_bajas_involuntarias_tot(vTablaBajasInv,vfecha_proceso))
    df_otc_t_bajas_involuntarias_del = spark.sql(del_otc_t_bajas_involuntarias(vTablaBajasInv,vfecha_proceso,vdia1_mes))
    df_otc_t_bajas_involuntarias_fin = df_otc_t_bajas_involuntarias_tot.join(df_otc_t_bajas_involuntarias_del, on=['proces_date','num_telefonico'], how='left_anti')
    columns = spark.table(vTablaBajasInv).columns
    cols = []
    for column in columns:
        cols.append(column)                
    df_otc_t_bajas_involuntarias_fin = df_otc_t_bajas_involuntarias_fin.select(cols)
    df_otc_t_bajas_involuntarias_fin.repartition(1).write.mode("overwrite").insertInto(vTablaBajasInv, overwrite=True)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaBajasInv))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_bajas_involuntarias_fin",str(df_otc_t_bajas_involuntarias_fin.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_bajas_involuntarias_fin",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df_otc_t_bajas_involuntarias_fin,df_otc_t_bajas_involuntarias_tot,df_otc_t_bajas_involuntarias_del
    
    vStp01="Paso 3"
    print(lne_dvs())
    print(etq_info("Paso [3]: Ejecucion de funcion [otc_t_2tmp_bajas_involuntarias]"))
    print(lne_dvs())
    df_otc_t_2tmp_bajas_involuntarias=spark.sql(otc_t_2tmp_bajas_involuntarias(vTablaBajasInv,vfecha_proceso,vdia1_mes)).cache()
    df_otc_t_2tmp_bajas_involuntarias.printSchema()
    ts_step_tbl = datetime.now()
    df_otc_t_2tmp_bajas_involuntarias.write.mode("overwrite").format("orc").saveAsTable(vTablaBajaInvTmp)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaBajaInvTmp))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_2tmp_bajas_involuntarias",str(df_otc_t_2tmp_bajas_involuntarias.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_2tmp_bajas_involuntarias",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 4"
    print(lne_dvs())
    print(etq_info("Paso [4]: Ejecucion de funcion [del_otc_t_bajas_involuntarias2]- Eliminando datos de bajas involuntarias"))
    print(lne_dvs())
    df_otc_t_bajas_involuntarias_mes = spark.sql(otc_t_bajas_involuntarias_mes(vTablaBajasInv,vfecha_proceso,vdia1_mes))
    df_otc_t_bajas_involuntarias2=spark.sql(del_otc_t_bajas_involuntarias2(vTablaBajasInv,vfecha_proceso,vdia1_mes,vTablaBajaInvTmp)).cache()
    if df_otc_t_bajas_involuntarias2.rdd.isEmpty():
        print(etq_info("No existen datos para eliminar"))
    else:
        columns = spark.table(vTablaBajasInv).columns
        cols = []
        for column in columns:
            cols.append(column)                
        df_otc_t_bajas_involuntarias_finmes = df_otc_t_bajas_involuntarias_finmes.select(cols)
        df_otc_t_bajas_involuntarias_finmes = df_otc_t_bajas_involuntarias_mes.join(df_otc_t_bajas_involuntarias2, on=['proces_date','num_telefonico'], how='left_anti')
        df_otc_t_bajas_involuntarias_finmes.printSchema()
        ts_step_tbl = datetime.now()
        df_otc_t_bajas_involuntarias_finmes.repartition(1).write.mode("overwrite").insertInto(vTablaBajasInv, overwrite=True)
        print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaBajasInv))) 
        print(etq_info(msg_t_total_registros_hive("df_otc_t_bajas_involuntarias_finmes",str(df_otc_t_bajas_involuntarias_finmes.count())))) #BORRAR
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive("df_otc_t_bajas_involuntarias2",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 5"
    print(lne_dvs())
    print(etq_info("Paso [5]: Ejecucion de funcion [del_otc_t_bajas_involuntarias3]- Eliminando datos de bajas involuntarias"))
    print(lne_dvs())
    df_otc_t_bajas_involuntarias_mes2 = spark.sql(otc_t_bajas_involuntarias_mes(vTablaBajasInv,vfecha_proceso,vdia1_mes))
    df_otc_t_bajas_involuntarias3=spark.sql(del_otc_t_bajas_involuntarias3(vTablaBajasInv,vfecha_proceso,vdia1_mes)).cache()
    if df_otc_t_bajas_involuntarias3.rdd.isEmpty():
        print(etq_info("No existen datos para eliminar"))
    else:
        columns = spark.table(vTablaBajasInv).columns
        cols = []
        for column in columns:
            cols.append(column)                
        df_otc_t_bajas_involuntarias_finmes2 = df_otc_t_bajas_involuntarias_finmes2.select(cols)
        df_otc_t_bajas_involuntarias_finmes2 = df_otc_t_bajas_involuntarias_mes2.join(df_otc_t_bajas_involuntarias3, on=['proces_date','num_telefonico'], how='left_anti')
        df_otc_t_bajas_involuntarias3.printSchema()
        ts_step_tbl = datetime.now()
        df_otc_t_bajas_involuntarias3.repartition(1).write.mode("overwrite").insertInto(vTablaBajasInv, overwrite=True)
        print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaBajasInv))) 
        print(etq_info(msg_t_total_registros_hive("df_otc_t_bajas_involuntarias3",str(df_otc_t_bajas_involuntarias3.count())))) #BORRAR
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive("df_otc_t_bajas_involuntarias3",vle_duracion(ts_step_tbl,te_step_tbl))))
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()    
    del df_otc_t_2tmp_bajas_involuntarias
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

