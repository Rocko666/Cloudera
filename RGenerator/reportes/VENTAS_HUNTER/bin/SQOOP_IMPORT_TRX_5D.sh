#########################################################################################################################
# MODIFICACIONES													#
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2022-12-04			Luis Fernando Llanganate		Se migra conforme a defininciones		#
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
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`

VAL_TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDUSER_NAPM_SCL01REP';"`
VAL_TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDPASS_NAPM_SCL01REP';"`
VAL_TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDHOST_NAPM_SCL01REP';"`
VAL_TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDPORT_NAPM_SCL01REP';"`
VAL_TDSERVICE=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'TDSERVICE_NAPM_SCL01REP';"`

# PARAMETROS ESPECIFICOS DEL SHELL MIGRADO
VAL_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SHELL2';"`
VAL_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON2';"`
VAL_HIVEDB1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE2';"`
VAL_HIVEDB2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE3';"`
VAL_HIVETABLE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE2';"`
VAL_HIVETABLE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE3';"`
VAL_COLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'cola_ejecucion';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ruta';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_CORE_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORE_EXECUTORS';"`
VAL_COLUMNA_PARTICION=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COLUMNA_PARTICION';"`
VAL_NUMERO_DIAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUMERO_DIAS';"`
VAL_POST_FIJO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_POST_FIJO';"`
VAL_TDBORACLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBORACLE';"`
VAL_TTBLORACLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLORACLE';"`

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
|| [ -z "$VAL_COLUMNA_PARTICION" ] 
|| [ -z "$VAL_NUMERO_DIAS" ] 
|| [ -z "$VAL_POST_FIJO" ] 
|| [ -z "$VAL_TDBORACLE" ] 
|| [ -z "$VAL_TTBLORACLE" ] ; then
	echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
	error=1
	exit $error
fi


#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@//$VAL_TDHOST:$VAL_TDPORT/$VAL_TDSERVICE
VAL_HIVETABLE1=$VAL_HIVETABLE1$VAL_POST_FIJO
VAL_HIVETABLE2=$VAL_HIVETABLE2$VAL_POST_FIJO


ini_fecha=`date '+%Y%m%d%H%M%S'`
VAL_LOG=${VAL_RUTA}/logs/sqoop_import_TRX_5D_$ini_fecha.log
f_proceso=`date -d "$fecha_reporte -1 days" "+%Y%m%d"`
n_dias=$VAL_NUMERO_DIAS
f_inicio=`date -d "$f_proceso -$n_dias days" "+%Y%m%d"`
echo $f_inicio
VAL_JARS=$VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11
echo $VAL_JARS

echo "Se inicia proceso de carga de datos desde PySpark"  2>&1 &>> $VAL_LOG

#SPARK DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
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
--vhivebd1=$VAL_HIVEDB1 \
--vtablahive1=$VAL_HIVETABLE1 \
--vhivebd2=$VAL_HIVEDB2 \
--vtablahive2=$VAL_HIVETABLE2 \
--vtipocarga=$VAL_TIPO_CARGA  \
--vfechainicio=$f_inicio  \
--vcolumnapt=$VAL_COLUMNA_PARTICION \
--vtabladef=$VAL_HIVETABLE1  \
--vfechafin=$f_proceso \
--vtdboracle=$VAL_TDBORACLE \
--vttbloracle=$VAL_TTBLORACLE 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion de la importacion TRX 5D EXITOSA ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - La ejecucion de la importacion TRX 5D Fallo ====" 2>&1 &>> $VAL_LOG
	exit 1
fi


