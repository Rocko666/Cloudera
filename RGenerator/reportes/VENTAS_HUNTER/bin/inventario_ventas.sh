#########################################################################################################################
# MODIFICACIONES													#
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2022-12-06			Luis Fernando Llanganate		Se migra conforme a defininciones		#
#########################################################################################################################
##############
# VARIABLES  #
##############
ENTIDAD=VENTAS_HUNTER

# PARAMETROS GENERICOS SPARK OBTENIDOS DE LA LA TABLA params_des
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params  where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'VAL_RUTA_SPARK';"`

# PARAMETROS ESPECIFICOS DEL SHELL MIGRADO
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$KINIT
VAL_SHELL=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SHELL4';"`
VAL_PYTHON=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON4';"`
VAL_MASTER=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_CORE_EXECUTORS=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORE_EXECUTORS';"`
VAL_RUTA=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ruta';"`
VAL_DBTGL=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE6';"`
VAL_TBTGL=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE6';"`
VAL_DBOVST5T2=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE7';"`
VAL_TBOVST5T2=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE7';"`
VAL_DBOVST5T3=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE8';"`
VAL_TBOVST5T3=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE8';"`
VAL_DBOTVHT=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE9';"`
VAL_TBOTVHT=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE9';"`
VAL_DBOTIHT1=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE10';"`
VAL_TBOTIHT1=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE10';"`
VAL_DBOTCV=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE11';"`
VAL_TBOTCV=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE11';"`
VAL_DBOTCVD=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE12';"`
VAL_TBOTCVD=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE12';"`
VAL_DBOTVHA=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE13';"`
VAL_TBOTVHA=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE13';"`
VAL_DBOTVHAR=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE14';"`
VAL_TBOTVHAR=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE14';"`
VAL_POST_FIJO=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_POST_FIJO';"`
VAL_ftp_puerto=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_puerto';"`
VAL_ftp_user=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_user';"`
VAL_ftp_hostname=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_hostname';"`
VAL_ftp_pass=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_pass';"`
VAL_ftp_path_envio=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_path_envio';"`

#Validacion de parametros iniciales, nulos y existencia de Rutas
if [ -z "$VAL_SHELL" ] 
|| [ -z "$VAL_PYTHON" ] 
|| [ -z "$VAL_MASTER" ] 
|| [ -z "$VAL_DRIVER_MEMORY" ] 
|| [ -z "$VAL_EXECUTOR_MEMORY" ] 
|| [ -z "$VAL_NUM_EXECUTORS" ] 
|| [ -z "$VAL_CORE_EXECUTORS" ] 
|| [ -z "$VAL_RUTA" ] 
|| [ -z "$VAL_DBTGL" ] 
|| [ -z "$VAL_TBTGL" ] 
|| [ -z "$VAL_DBOVST5T2" ] 
|| [ -z "$VAL_TBOVST5T2" ] 
|| [ -z "$VAL_DBOVST5T3" ] 
|| [ -z "$VAL_TBOVST5T3" ]  
|| [ -z "$VAL_DBOTVHT" ] 
|| [ -z "$VAL_TBOTVHT" ] 
|| [ -z "$VAL_DBOTIHT1" ] 
|| [ -z "$VAL_TBOTIHT1" ] 
|| [ -z "$VAL_DBOTCV" ] 
|| [ -z "$VAL_TBOTCV" ]  
|| [ -z "$VAL_DBOTVHA" ] 
|| [ -z "$VAL_TBOTVHA" ] 
|| [ -z "$VAL_DBOTVHAR" ] 
|| [ -z "$VAL_TBOTVHAR" ] 
|| [ -z "$VAL_POST_FIJO" ] 
|| [ -z "$VAL_ftp_puerto" ]  
|| [ -z "$VAL_ftp_user" ] 
|| [ -z "$VAL_ftp_hostname" ] 
|| [ -z "$VAL_ftp_pass" ] 
|| [ -z "$VAL_ftp_path_envio" ] ; then
	echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
	error=1
	exit $error
fi

VAL_ftp_path_envio=$VAL_ftp_path_envio/nae109514

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@//$VAL_TDHOST:$VAL_TDPORT/$VAL_TDSERVICE
echo $VAL_DBTGL
VAL_TBTGL=$VAL_TBTGL$VAL_POST_FIJO
VAL_TBOVST5T2=$VAL_TBOVST5T2$VAL_POST_FIJO
VAL_TBOVST5T3=$VAL_TBOVST5T3$VAL_POST_FIJO
VAL_TBOTVHT=$VAL_TBOTVHT$VAL_POST_FIJO
VAL_TBOTIHT1=$VAL_TBOTIHT1$VAL_POST_FIJO
VAL_TBOTCV=$VAL_TBOTCV$VAL_POST_FIJO
VAL_TBOTCVD=$VAL_TBOTCVD$VAL_POST_FIJO
VAL_TBOTVHA=$VAL_TBOTVHA$VAL_POST_FIJO
VAL_TBOTVHAR=$VAL_TBOTVHAR$VAL_POST_FIJO

ini_fecha=`date '+%Y%m%d%H%M%S'`
fecha_proceso=`date '+%Y%m%d'`

VAL_LOG=${VAL_RUTA}/logs/inventario_ventas_$ini_fecha.log

echo "Se inicia proceso de carga de datos desde PySpark de tablas temporales "  2>&1 &>> $VAL_LOG

#SPARK DE ORACLE A HIVE CARGA DE LAS TABLAS TEMPORALES

$VAL_RUTA_SPARK \
 
--conf spark.shuffle.service.enabled=false \

--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_CORE_EXECUTORS \
--driver-memory $VAL_DRIVER_MEMORY \
  $VAL_RUTA/python/$VAL_PYTHON \
--ventidad=$ENTIDAD \
--vDBTGL=$VAL_DBTGL --vTBTGL=$VAL_TBTGL \
--vDBOVST5T2=$VAL_DBOVST5T2 --vTBOVST5T2=$VAL_TBOVST5T2 \
--vDBOVST5T3=$VAL_DBOVST5T3 --vTBOVST5T3=$VAL_TBOVST5T3 \
--vDBOTVHT=$VAL_DBOTVHT --vTBOTVHT=$VAL_TBOTVHT \
--vDBOTIHT1=$VAL_DBOTIHT1 --vTBOTIHT1=$VAL_TBOTIHT1 \
--vDBOTCV=$VAL_DBOTCV --vTBOTCV=$VAL_TBOTCV \
--vDBOTCVD=$VAL_DBOTCVD --vTBOTCVD=$VAL_TBOTCVD \
--vDBOTVHA=$VAL_DBOTVHA --vTBOTVHA=$VAL_TBOTVHA \
--vDBOTVHAR=$VAL_DBOTVHAR --vTBOTVHAR=$VAL_TBOTVHAR \
--rutaTXT=${VAL_RUTA}/Output/ \
--formatotxt=$fecha_proceso.txt  2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion de la importacion CARGA DE LAS TABLAS TEMPORALES  ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - La ejecucion de la importacion CARGA DE LAS TABLAS TEMPORALES  ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

#SPARK DE ORACLE A HIVE CARGA DE LAS TABLAS TEMPORALES

#--Llevar desde bigdata9 hacia ftp
    ftp -inv  << EOF
    open ${VAL_ftp_hostname} ${VAL_ftp_puerto}
    passive
    bin   
    quote USER ${VAL_ftp_user}
    quote PASS ${VAL_ftp_pass}
    cd ${VAL_ftp_path_envio}/
    lcd ${VAL_RUTA}/Output/
    put Ventas_Acumuladas_$fecha_proceso.txt
    put Inventario_Hunter_$fecha_proceso.txt
    put Ventas_individuales_$fecha_proceso.txt
EOF

fecha_hoy=`date '+%Y%m%d'`
fecha_params=`date -d "$fecha_hoy +1 days" "+%Y%m%d"`

`mysql -N  <<<"UPDATE params  SET VALOR=20221224 WHERE  ENTIDAD='"$ENTIDAD"' AND PARAMETRO='VAL_FECHA_REPORTE';"`
