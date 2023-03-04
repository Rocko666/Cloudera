#!/bin/bash
########################################################################################################
# NOMBRE: OTC_T_OFERTA_SUGERIDA_PARQUE.sh                                                              #
# DESCRIPCIoN:                                                                                         #
# Shell que realiza el llamado a la consulta de las fuentes con informacion de la oferta sugerida      #
# AUTOR: Xavier Vera - Softconsulting                                                                  #
# FECHA CREACIoN: 2022/01/24                                                                           #
# PARAMETROS DEL SHELL                                                                                 #
# PARAM1_FECHA_EJEC=${1}    Fecha de ejecucion de proceso en formato  YYYYMMDD                         #
# VAL_IMPUESTOS=${2}        Valor de Impuestos aplica al campo TARIFA_BASICA_ACTUAL                    #
# VAL_COLA_EJECUCION=${3}   Cola utilizada para ejecutar el proceso                                    #
# VAL_CADENA_JDBC=${4}      Cadena de conexion a Hive utilizada para el proceso                        #
# VAL_RUTA=${5}             Usuario utilizado para ejecucion del proceso                               #
########################################################################################################
##########################################################################
# MODIFICACIONES
# FECHA  		     AUTOR     		      DESCRIPCION MOTIVO
# 2022-12-30	  Rodrigo Sandoval	  Migracion a spark
##########################################################################
##########################################################################
# MODIFICACIONES
# FECHA  		     AUTOR     		      DESCRIPCION MOTIVO
# 2023-02-13	  Luis Llanganate	  Revision en Laboratorio
##########################################################################

ENTIDAD=BIOFRTSGRD0010
VAL_FECHA_PROCESO=$1

#VAL_IMPUESTOS=$2
#VAL_COLA_EJECUCION=$3
#VAL_CADENA_JDBC=$4
#VAL_RUTA=$5
#VAL_USER=$6

VAL_IMPUESTOS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM2_VAL_IMPUESTOS';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM3_VAL_COLA_EJECUCION';"`
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM4_VAL_CADENA_JDBC';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM5_VAL_RUTA';"`
VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM6_VAL_USER';"`

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDCLASS_ORC';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`
VAL_NOM_JAR_ORC_19=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_19';"`

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

#PARAMETROS GENERICOS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER='nae108835'

#VALIDACION DE PARAMETROS DE LA TABLA params
if [ -z "$VAL_RUTA" ] || [ -z "$SHELL" ] || [ -z "$RUTA_PYTHON" ] ||
	[ -z "$VAL_MASTER" ] || [ -z "$VAL_DRIVER_MEMORY" ] || [ -z "$VAL_EXECUTOR_MEMORY" ] || [ -z "$VAL_NUM_EXECUTORS" ] || [ -z "$VAL_NUM_EXECUTORS_CORES" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales esta vacio o nulo (tabla params)"
	exit 1
fi

#________________________________________________
# PARAMETROS DE SHELL DEL PROCESO ANTERIOR
#________________________________________________

#PARAMETROS DEFINIDOS EN LA TABLA params
CORREOS_ADVERTENCIA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'CORREOS_ADVERTENCIA';"`
TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TDUSER' );"`
TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TDPASS' );"`
TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TDHOST' );"`
TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"`
TDDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TDDB' );"`
TDTABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLA';"`
TDTABLA_PREPAGO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLA_PREPAGO';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
VAL_SQL_PYSPARK1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SQL_PYSPARK1';"`
VAL_SQL_PYSPARK2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SQL_PYSPARK2';"`
VAL_EXPORT_PYSPARK1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXPORT_PYSPARK1';"`
VAL_EXPORT_PYSPARK2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXPORT_PYSPARK2';"`
VAL_COUNTED_DAYS_INI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COUNTED_DAYS_INI';"`
VAL_COUNTED_DAYS_87_FIN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COUNTED_DAYS_87_FIN';"`
VAL_COUNTED_DAYS_88_FIN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COUNTED_DAYS_88_FIN';"`

#VARIABLES CALCULADAS Y AUTOGENERADAS
VAL_FEC_2_DIAS_CAIDO=`date -d "${VAL_FECHA_PROCESO} -2 day"  +"%Y%m%d"`
VAL_FEC_3_DIAS_CAIDO=`date -d "${VAL_FECHA_PROCESO} -3 day"  +"%Y%m%d"`
VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_FECHA_ACTUAL=`date '+%Y%m%d'`
VAL_LOG_EJECUCION=$VAL_RUTA"/log/SH_OTC_T_OFERTA_SUGERIDA_PARQUE"$VAL_HORA".log"

JDBCURL=jdbc:oracle:thin:@//$TDHOST:$TDPORT/$TDDB
HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar

ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"';"`

if ! [ "$ExisteEntidad" -gt 0 ]; then #-gt mayor a -lt menor a
	echo " $TIME [ERROR] $rc No existen parametros para la entidad $ENTIDAD"
	((rc=1))
	exit $rc
fi

#Validacion de parametros iniciales, nulos y existencia de Rutas
if [ -z "$VAL_FECHA_PROCESO" ] 
|| [ -z "$VAL_IMPUESTOS" ] 
|| [ -z "$VAL_COLA_EJECUCION" ] 
|| [ -z "$VAL_CADENA_JDBC" ] 
|| [ -z "$VAL_RUTA" ] 
|| [ -z "$VAL_USER" ] 
|| [ -z "$CORREOS_ADVERTENCIA" ] 
|| [ -z "$TDUSER" ] 
|| [ -z "$TDPASS" ] 
|| [ -z "$TDHOST" ] 
|| [ -z "$TDPORT" ] 
|| [ -z "$TDDB" ] 
|| [ -z "$TDTABLA" ] 
|| [ -z "$TDTABLA_PREPAGO" ] 
|| [ -z "$ETAPA" ] 
|| [ -z "$VAL_SHELL" ] 
|| [ -z "$VAL_SQL_PYSPARK1" ] 
|| [ -z "$VAL_LOG_EJECUCION" ]; then
  echo " $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo"
  error=2
  exit $error
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso de generacion de archivos OFERTA SUGERIDA PARQUE APP  ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
echo "Fecha Proceso: $VAL_FECHA_PROCESO" 2>&1 &>> $VAL_LOG_EJECUCION
echo "Impuestos: $VAL_IMPUESTOS" 2>&1 &>> $VAL_LOG_EJECUCION
echo "==== Inicio Sub-Proceso ETAPA1 ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

#BORRAR EN PRODUCCION (DESARROLLO)
ETAPA=1

#________________________________________________
# ETAPA 1
#________________________________________________

if [ "$ETAPA" = "1" ]; then

#------------------------------------------------------#
# DEFINICION DE FECHAS                                 #
#------------------------------------------------------#
#SI ES AS  LE OBTIENE LA M XIMA PARTICIoN DE LA TABLA py_upsell fecha actual

VAL_FECHA_INICIO_VALIDA=`date '+%Y-%m-%d' -d "$VAL_FECHA_PROCESO-5 day"`

VAL_FECHA_MAX_UPSELL=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "select max(fecha_proceso) from db_thebox.py_upsell where fecha_proceso > '$VAL_FECHA_INICIO_VALIDA'; ") 2>&1 &>> $VAL_LOG_EJECUCION

echo "==== Termina el calculo de la fecha de proceso ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

VAL_FECHA_MAX_UPSELL=`echo $VAL_FECHA_MAX_UPSELL|sed "s/\NULL//g"`

echo "VAL_FECHA_MAX_UPSELL: $VAL_FECHA_MAX_UPSELL" 2>&1 &>> $VAL_LOG_EJECUCION
echo "VAL_FECHA_INICIO_VALIDA: $VAL_FECHA_INICIO_VALIDA" 2>&1 &>> $VAL_LOG_EJECUCION

echo "VAL_FECHA_PROCESO: $VAL_FECHA_PROCESO" 2>&1 &>> $VAL_LOG_EJECUCION


if [ -z "$VAL_FECHA_MAX_UPSELL" ]; then

   echo "El proceso no continua con fecha $VAL_FECHA_PROCESO porque se debe actualizar la informacion de la tabla py_upsell (tiene 5 o mas dias sin ser actualizada)" 2>&1 &>> $VAL_LOG_EJECUCION

/usr/sbin/sendmail -vt <<EOF
To: $CORREOS_ADVERTENCIA
Subject: Aviso Informacion desactualizada en la tabla py_upsell del proceso OTC_T_OFERTA_SUGERIDA_PARQUE BIGD413, de oferta sugerida mediante la APP
Advertencia NO se ejecutara el proceso de extraccion de oferta sugerida mediante la APP, con fecha de proceso $VAL_FECHA_PROCESO por la informacion de la tabla py_upsell
EOF
  exit 1
fi

VAL_FECHA_MAX_UPSELL=`echo $VAL_FECHA_MAX_UPSELL|sed "s/\-//g"`
echo "VAL_FECHA_MAX_UPSELL: $VAL_FECHA_MAX_UPSELL" 2>&1 &>> $VAL_LOG_EJECUCION

VAL_FECHA_INI=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO-4 day"`

echo "VAL_FECHA_INI: $VAL_FECHA_INI" 2>&1 &>> $VAL_LOG_EJECUCION
echo "VAL_FECHA_ACTUAL: $VAL_FECHA_ACTUAL" 2>&1 &>> $VAL_LOG_EJECUCION
echo "VAL_FECHA_PROCESO: $VAL_FECHA_PROCESO" 2>&1 &>> $VAL_LOG_EJECUCION

#IDENTIFICA SI LA FECHA DE PROCESO ES LA FECHA ACTUAL
if [ $VAL_FECHA_ACTUAL = $VAL_FECHA_PROCESO ]; then

#VALIDO SI ESTA DENTRO DEL RANGO DE ADVERTENCIA (DE 2 A 4 DIAS)
#2022-11-03 SE CALCULAN FECHAS
VAL_FECHA_PROCESO_NEW=`date -d "${VAL_FECHA_PROCESO}"  +"%Y-%m-%d"`
VAL_FECHA_MAX_UPSELL_NEW=`date -d "${VAL_FECHA_MAX_UPSELL}"  +"%Y-%m-%d"`
DIAS=$((($(date -d "$(date -d $VAL_FECHA_PROCESO_NEW +%Y-%m-%d)" "+%s") - $(date -d "$(date -d $VAL_FECHA_MAX_UPSELL_NEW +%Y-%m-%d)" "+%s")) / 86400))

echo "Esta dentro del rango de dias de advertencia, el dia $DIAS"

if [ $DIAS -gt 2 ]; then
      echo "Advertencia se ejecutara el proceso con fecha $VAL_FECHA_PROCESO pero debe actualizar la informacion de la tabla py_upsell la ultima fecha de actualizacion es: $VAL_FECHA_MAX_UPSELL" 2>&1 &>> $VAL_LOG_EJECUCION
      echo "Esta dentro del rango de dias de advertencia, el dia $DIAS" 2>&1 &>> $VAL_LOG_EJECUCION


/usr/sbin/sendmail -vt <<EOF
To: $CORREOS_ADVERTENCIA
Subject: Aviso Informacion desactualizada en la tabla py_upsell del proceso OTC_T_OFERTA_SUGERIDA_PARQUE BIGD413, de oferta sugerida mediante la APP
Advertencia se ejecutara el proceso de oferta sugerida mediante la APP, con fecha de proceso $VAL_FECHA_PROCESO pero debe actualizar la informacion de la tabla py_upsell, dado que la ultima fecha de actualizacion es: $VAL_FECHA_MAX_UPSELL
Esta dentro del rango de dias de advertencia, el dia $DIAS
EOF

fi

fi

#_______________________________________________________________
# PROCESO PYSPARK DE LOGICA DE script_extrae_oferta_sugerida.sql
#_______________________________________________________________

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Inicia PROCESO PYSPARK DE LOGICA DE script_extrae_oferta_sugerida.sql" 2>&1 &>> $VAL_LOG_EJECUCION
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Conexion a Oracle: $JDBCURL" 2>&1 &>> $VAL_LOG_EJECUCION


#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_19 \
$RUTA_PYTHON/$VAL_SQL_PYSPARK1 \
--vclass=$TDCLASS_ORC \
--vjdbcurl=$JDBCURL \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vfecha_proceso $VAL_FECHA_PROCESO \
--vimpuestos $VAL_IMPUESTOS 2>&1 &>> $VAL_LOG_EJECUCION


#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG_EJECUCION | wc -l`
if [ $error_spark -eq 0 ];then
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: La ejecucion del archivo spark $VAL_SQL_PYSPARK1 es EXITOSO " 2>&1 &>> $VAL_LOG_EJECUCION
else
echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: En la ejecucion del archivo spark $VAL_SQL_PYSPARK1" 2>&1 &>> $VAL_LOG_EJECUCION
exit 1
fi


# Valida si existe error al ejecutar el script de Hive Sql que genera los datos
error_carga=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex' $VAL_LOG_EJECUCION | wc -l`
if [ $error_carga -eq 0 ];then
	echo " === ...FIN Sub-Proceso ETAPA1... === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
else
	echo " === ERROR en Sub-Proceso ETAPA1 === " 2>&1 &>> $VAL_LOG_EJECUCION
	exit 1
fi



  ETAPA=2
  `mysql -N  <<<"update params set valor=$ETAPA where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`

fi


#________________________________________________
# ETAPA 2
#________________________________________________


if [ "$ETAPA" = "2" ]; then
echo " === INICIA ETAPA $ETAPA EXPORTACION A ORACLE TABLA OTC_T_OFERTA_PERSONAL === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
echo " === INICIA ETAPA $ETAPA EXPORTACION A HDFS TABLA OTC_T_OFERTA_PERSONAL === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

echo " === INICIA PYSPARK EXPORT DE TABLA $TDUSER.$TDTABLA === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

#________________________________________________
# INICIA PYSPARK EXPORT
#________________________________________________

#EJECUTA PYSPARK EXPORT
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master yarn \
--executor-memory 2G \
--num-executors 80 \
--executor-cores 4 \
--driver-memory 2G \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_19 $RUTA_PYTHON/$VAL_EXPORT_PYSPARK1 \
--vJdbcUrl $JDBCURL \
--vTDTable $TDTABLA \
--vTDUser $TDUSER \
--vTDPass $TDPASS 2>&1 &>> $VAL_LOG_EJECUCION

#VALIDA EJECUCION DEL HQL
VAL_ERRORES=`egrep 'OK - PROCESO PYSPARK-EXPORT1 TERMINADO' $VAL_LOG_EJECUCION | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
    error=3
    echo "=== ERROR en el overwrite de la tabla $TDUSER.$TDTABLA" 2>&1 &>> $VAL_LOG_EJECUCION
	exit $error
else
    error=0
	echo "==== Overwrite EXITOSO de tabla $TDUSER.$TDTABLA ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
fi


ETAPA=3
`mysql -N  <<<"update params set valor=$ETAPA where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`

fi


#________________________________________________
# ETAPA 3
#________________________________________________

#2022-11-03 SE AGREGAN ESTAS ETAPAS QUE SIGUEN PARA LA TABLA OTC_T_OFERTA_PREPAGO
if [ "$ETAPA" = "3" ]; then
echo " === INICIA ETAPA $ETAPA GENERA INFORMACION TABLA OTC_T_OFERTA_PREPAGO === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
echo " === Valida si la tabla otc_t_rtd_oferta_sugerida contiene datos === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
VAL_FEC_AYER=`date -d "${VAL_FECHA_PROCESO} -1 day"  +"%Y%m%d"` 2>&1 &>> $VAL_LOG_EJECUCION
VAL_DATA_OFERTA_SUG=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "select count(1) FROM db_reportes.otc_t_rtd_oferta_sugerida where fecha_proceso=$VAL_FEC_AYER; ") 2>&1 &>> $VAL_LOG_EJECUCION
echo "La tabla otc_t_rtd_oferta_sugerida contiene $VAL_DATA_OFERTA_SUG registros. " 2>&1 &>> $VAL_LOG_EJECUCION

	if [ $VAL_DATA_OFERTA_SUG -gt 0 ];then
	VAL_FECHA_CHURN=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "select MAX(proces_date) FROM db_cs_altas.otc_t_churn_sp2 where proces_date>=$VAL_FEC_3_DIAS_CAIDO AND proces_date<=$VAL_FEC_2_DIAS_CAIDO; ") 2>&1 &>> $VAL_LOG_EJECUCION

		if [ $VAL_FECHA_CHURN -eq $VAL_FEC_2_DIAS_CAIDO ];then
			echo "La tabla db_cs_altas.otc_t_churn_sp2 tiene 2 dias caido, se tienen en cuenta los registros con counted_days entre $VAL_COUNTED_DAYS_INI y $VAL_COUNTED_DAYS_88_FIN" 2>&1 &>> $VAL_LOG_EJECUCION
			fecha_churn=$VAL_FECHA_CHURN
			dias_desde=$VAL_COUNTED_DAYS_INI
			dias_hasta=$VAL_COUNTED_DAYS_88_FIN
			else
				if [ $VAL_FECHA_CHURN -eq $VAL_FEC_3_DIAS_CAIDO ];then
					echo "La tabla db_cs_altas.otc_t_churn_sp2 tiene 3 dias caido, se tienen en cuenta los registros con counted_days entre $VAL_COUNTED_DAYS_INI y $VAL_COUNTED_DAYS_87_FIN" 2>&1 &>> $VAL_LOG_EJECUCION
					fecha_churn=$VAL_FECHA_CHURN
					dias_desde=$VAL_COUNTED_DAYS_INI
					dias_hasta=$VAL_COUNTED_DAYS_87_FIN
				else
					echo "ERROR - La tabla db_cs_altas.otc_t_churn_sp2, tiene mas de 3 dias caido. " 2>&1 &>> $VAL_LOG_EJECUCION
					exit 1
				fi
		fi

	echo "Ejecuta HQL otc_t_oferta_prepago que genera inforacion de OTC_T_OFERTA_PREPAGO. " 2>&1 &>> $VAL_LOG_EJECUCION
	#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_19 \
$RUTA_PYTHON/$VAL_SQL_PYSPARK2 \
--vclass=$TDCLASS_ORC \
--vjdbcurl=$JDBCURL \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vfecha_proceso $VAL_FECHA_PROCESO \
--vfecha_ayer $VAL_FEC_AYER \
--vfecha_churn $fecha_churn \
--vdias_desde $dias_desde \
--vdias_hasta $dias_hasta &2>&1 &>> $VAL_LOG_EJECUCION


#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG_EJECUCION | wc -l`
if [ $error_spark -eq 0 ];then
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: La ejecucion del archivo spark $VAL_SQL_PYSPARK2 es EXITOSO " 2>&1 &>> $VAL_LOG_EJECUCION
else
echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: En la ejecucion del archivo spark $VAL_SQL_PYSPARK2" 2>&1 &>> $VAL_LOG_EJECUCION
exit 1
fi

/usr/sbin/sendmail -vt <<EOF
To: $CORREOS_ADVERTENCIA
Subject: No se genera informacion actualizada en la tabla destino OTC_T_OFERTA_PREPAGO en Oracle
Advertencia No hay datos en la tabla fuente otc_t_rtd_oferta_sugerida en Hive. Por lo tanto, no se genera informacion actualizada en la tabla OTC_T_OFERTA_PREPAGO en Oracle. Se mantiene la informacion anterior.
EOF

	echo "== FIN OK PROCESO CARGA OFERTA SUGERIDA PARQUE APP =="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
	ETAPA=1
	`mysql -N  <<<"update params set valor=$ETAPA where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
	exit 0

fi

ETAPA=4
`mysql -N  <<<"update params set valor=$ETAPA where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi

#________________________________________________
# ETAPA 4
#________________________________________________

if [ "$ETAPA" = "4" ]; then
echo " === INICIA ETAPA $ETAPA EXPORTACION A ORACLE TABLA OTC_T_OFERTA_PREPAGO === "`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION


#________________________________________________
# INICIA PYSPARK EXPORT
#________________________________________________

#EJECUTA PYSPARK EXPORT
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master yarn \
--executor-memory 2G \
--num-executors 80 \
--executor-cores 4 \
--driver-memory 2G \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_19 $RUTA_PYTHON/$VAL_EXPORT_PYSPARK2 \
--vJdbcUrl $JDBCURL \
--vTDTable $TDTABLA_PREPAGO \
--vTDUser $TDUSER \
--vTDPass $TDPASS 2>&1 &>> $VAL_LOG_EJECUCION

#VALIDA EJECUCION DEL HQL
VAL_ERRORES=`egrep 'OK - PROCESO PYSPARK-EXPORT2 TERMINADO' $VAL_LOG_EJECUCION | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
    error=3
    echo "=== ERROR en el overwrite de la tabla $TDUSER.$TDTABLA_PREPAGO" 2>&1 &>> $VAL_LOG_EJECUCION
	exit $error
else
    error=0
	echo "==== Overwrite EXITOSO de tabla $TDUSER.$TDTABLA_PREPAGO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
fi

ETAPA=1
`mysql -N  <<<"update params set valor=$ETAPA where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`

fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Finaliza ejecucion del proceso $ENTIDAD" 2>&1 &>> $VAL_LOG_EJECUCION


exit $error


