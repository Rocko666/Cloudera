#########################################################################################################################
# MODIFICACIONES													#
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2022-12-06			Luis Fernando Llanganate		Se migra conforme a defininciones		#
#########################################################################################################################
##############
# VARIABLES  #
##############
ENTIDAD=VENTAS_HUNTER

# PARAMETROS GENERICOS SPARK OBTENIDOS DE LA LA TABLA params
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`

VAL_TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDUSER_NAPM_SCL01REP';"`
VAL_TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDPASS_NAPM_SCL01REP';"`
VAL_TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDHOST_NAPM_SCL01REP';"`
VAL_TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDPORT_NAPM_SCL01REP';"`
VAL_TDSERVICE=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDSERVICE_NAPM_SCL01REP';"`

# PARAMETROS ESPECIFICOS DEL SHELL MIGRADO
VAL_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SHELL3';"`
VAL_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON3';"`
VAL_HIVEDB1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE4';"`
VAL_HIVEDB2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE5';"`
VAL_HIVETABLE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE4';"`
VAL_HIVETABLE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE5';"`
VAL_COLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'cola_ejecucion';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ruta';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_CORE_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORE_EXECUTORS';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_POST_FIJO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_POST_FIJO';"`
VAL_TDBORACLE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBORACLE1';"`
VAL_TTBLORACLE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLORACLE1';"`
VAL_TDBORACLE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBORACLE2';"`
VAL_TTBLORACLE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLORACLE2';"`


#Validacion de parametros iniciales, nulos y existencia de Rutas
if [ -z "$VAL_SHELL" ] 
|| [ -z "$VAL_PYTHON" ] 
|| [ -z "$VAL_HIVEDB1" ] 
|| [ -z "$VAL_HIVEDB2" ] 
|| [ -z "$VAL_HIVETABLE1" ] 
|| [ -z "$VAL_HIVETABLE2" ] 
|| [ -z "$VAL_COLA" ] 
|| [ -z "$VAL_RUTA" ] 
|| [ -z "$VAL_TIPO_CARGA" ] 
|| [ -z "$VAL_MASTER" ] 
|| [ -z "$VAL_DRIVER_MEMORY" ] 
|| [ -z "$VAL_EXECUTOR_MEMORY" ] 
|| [ -z "$VAL_NUM_EXECUTORS" ] 
|| [ -z "$VAL_CORE_EXECUTORS" ]  
|| [ -z "$VAL_POST_FIJO" ] 
|| [ -z "$VAL_TDBORACLE1" ] 
|| [ -z "$VAL_TTBLORACLE1" ] 
|| [ -z "$VAL_TDBORACLE2" ] 
|| [ -z "$VAL_TTBLORACLE2" ] ; then
	echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
	error=1
	exit $error
fi


#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@//$VAL_TDHOST:$VAL_TDPORT/$VAL_TDSERVICE
VAL_HIVETABLE1=$VAL_HIVETABLE1$VAL_POST_FIJO
VAL_HIVETABLE2=$VAL_HIVETABLE2$VAL_POST_FIJO


ini_fecha=`date '+%Y%m%d%H%M%S'`
VAL_LOG=${VAL_RUTA}/logs/sqoop_import_ventas_hunter_$ini_fecha.log
VAL_JARS=$VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11
echo $VAL_JARS


# PASO 1. CARGA DE LA TABLE: OTC_V_PARTNER_SIM

echo "Se inicia proceso de carga de datos desde PySpark : CARGA DE LA TABLE: OTC_V_PARTNER_SIM "  2>&1 &>> $VAL_LOG

#SPARK DE ORACLE A HIVE CARGA DE LA TABLE: OTC_V_PARTNER_SIM

$VAL_RUTA_SPARK \
 
--conf spark.shuffle.service.enabled=false \

--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORE_EXECUTORS \
--driver-memory $VAL_DRIVER_MEMORY \
--jars  $VAL_JARS \
$VAL_RUTA/python/$VAL_PYTHON \
--ventidad=$ENTIDAD \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$VAL_TDUSER \
--vclavebd=$VAL_TDPASS \
--vhivebd=$VAL_HIVEDB1 \
--vtablahive=$VAL_HIVETABLE1 \
--vtipocarga=$VAL_TIPO_CARGA \
--vtdboracle=$VAL_TDBORACLE1 \
--vttbloracle=$VAL_TTBLORACLE1 \
--vFuncion=1  2>&1 &>> $VAL_LOG


#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion de la importacion CARGA DE LA TABLE: OTC_V_PARTNER_SIM ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - La ejecucion de la importacion CARGA DE LA TABLE: OTC_V_PARTNER_SIM ====" 2>&1 &>> $VAL_LOG
	exit 1
fi


# PASO 2. CARGA DE LA TABLE: OTC_V_SETTRANS_PM_PKT

echo "Se inicia proceso de carga de datos desde PySpark : CARGA DE LA TABLE: OTC_V_PARTNER_SIM "  2>&1 &>> $VAL_LOG

#SPARK DE ORACLE A HIVE CARGA DE LA TABLE: OTC_V_SETTRANS_PM_PKT

$VAL_RUTA_SPARK \
 
--conf spark.shuffle.service.enabled=false \

--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORE_EXECUTORS \
--driver-memory $VAL_DRIVER_MEMORY \
--jars  $VAL_JARS \
$VAL_RUTA/python/$VAL_PYTHON \
--ventidad=$ENTIDAD \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$VAL_TDUSER \
--vclavebd=$VAL_TDPASS \
--vhivebd=$VAL_HIVEDB2 \
--vtablahive=$VAL_HIVETABLE2 \
--vtipocarga=$VAL_TIPO_CARGA \
--vtdboracle=$VAL_TDBORACLE2 \
--vttbloracle=$VAL_TTBLORACLE2 \
--vFuncion=2  2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion de la importacion CARGA DE LA TABLE: OTC_V_SETTRANS_PM_PKT ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - La ejecucion de la importacion CARGA DE LA TABLE: OTC_V_SETTRANS_PM_PKT ====" 2>&1 &>> $VAL_LOG
	exit 1
fi
exit 0


