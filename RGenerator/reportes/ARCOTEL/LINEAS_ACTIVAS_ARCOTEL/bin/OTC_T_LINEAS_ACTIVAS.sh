#########################################################################################################################
# MODIFICACIONES													#
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2022-12-04			Luis Fernando Llanganate		Se migra conforme a defininciones		#
#########################################################################################################################
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2023-03-01			Cristian Ortiz		Inclusion & y manejo de errores, ambientacion a produccion para pruebas de  equipo operaciones		#
#                                           
#########################################################################################################################
##############
# VARIABLES  #
##############
ENTIDAD=LNSACTARCTL0010

# PARAMETROS GENERICOS SPARK OBTENIDOS DE LA LA TABLA params
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'VAL_RUTA_SPARK';"`

# PARAMETROS ESPECIFICOS DEL SHELL MIGRADO
VAL_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SHELL';"`
VAL_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON';"`
VAL_COLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_COLA';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
VAL_POST_FIJO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_POST_FIJO';"`
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
VAL_ESQUEMA_RPT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_RPT';"`

VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`

VAL_TDBHIVE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE1';"`
VAL_TTBLHIVE1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE1';"`
VAL_TDBHIVE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE2';"`
VAL_TTBLHIVE2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE2';"`
VAL_TDBHIVE3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE3';"`
VAL_TTBLHIVE3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE3';"`
VAL_TDBHIVE4=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE4';"`
VAL_TTBLHIVE4=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE4';"`
VAL_TDBHIVE5=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE5';"`
VAL_TTBLHIVE5=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE5';"`
VAL_TDBHIVE6=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE6';"`
VAL_TTBLHIVE6=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE6';"`
VAL_TDBHIVE7=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE7';"`
VAL_TTBLHIVE7=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE7';"`
VAL_TDBHIVE8=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE8';"`
VAL_TTBLHIVE8=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE8';"`
VAL_TDBHIVE9=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE9';"`
VAL_TTBLHIVE9=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE9';"`
VAL_TDBHIVE10=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE10';"`
VAL_TTBLHIVE10=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE10';"`
VAL_TDBHIVE11=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE11';"`
VAL_TTBLHIVE11=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE11';"`
VAL_TDBHIVE12=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE12';"`
VAL_TTBLHIVE12=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE12';"`


#PARAMETROS CALCULADOS Y AUTOGENERADOS

VAL_TTBLHIVE1=$VAL_TTBLHIVE1$VAL_POST_FIJO
VAL_TTBLHIVE2=$VAL_TTBLHIVE2$VAL_POST_FIJO
VAL_TTBLHIVE3=$VAL_TTBLHIVE3$VAL_POST_FIJO
VAL_TTBLHIVE4=$VAL_TTBLHIVE4$VAL_POST_FIJO
VAL_TTBLHIVE5=$VAL_TTBLHIVE5$VAL_POST_FIJO
VAL_TTBLHIVE6=$VAL_TTBLHIVE6$VAL_POST_FIJO
VAL_TTBLHIVE7=$VAL_TTBLHIVE7$VAL_POST_FIJO
VAL_TTBLHIVE8=$VAL_TTBLHIVE8$VAL_POST_FIJO
VAL_TTBLHIVE9=$VAL_TTBLHIVE9$VAL_POST_FIJO
VAL_TTBLHIVE10=$VAL_TTBLHIVE10$VAL_POST_FIJO
VAL_TTBLHIVE11=$VAL_TTBLHIVE11$VAL_POST_FIJO
VAL_TTBLHIVE12=$VAL_TTBLHIVE12$VAL_POST_FIJO


VAL_FECHA_PROCESO=$1
VAL_COLA_EJECUCION='capa_semantica'

#PARAMETROS CALCULADOS Y AUTOGENERADOS
HORA=`date '+%Y%m%d%H%M%S'`
VAL_YEAR=`echo $VAL_FECHA_PROCESO | cut -c1-4`
VAL_MONTH=`echo $VAL_FECHA_PROCESO | cut -c5-6`
VAL_DAY=`echo $VAL_FECHA_PROCESO | cut -c7-8`

FECHAEJE=$VAL_FECHA_PROCESO

eval year=`echo $FECHAEJE | cut -c1-4` #SUBSTRING 
eval month=`echo $FECHAEJE | cut -c5-6` #SUBSTRING 
day="01"

VAL_FEC_INI_MES=`date '+%Y%m01' -d "$FECHAEJE"` #Fecha de inicio del mes de ejecucion Formato YYYYMMDD: 20210801
VAL_FECHA_INI_PROC=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -4 month"` #Fecha de inicio del mes de ejecucion menos 4 para calculo del trafico Formato YYYYMMDD: 20210801
VAL_FECHA_INI_MES_PROC=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -1 month"` #Fecha que tiene la particion de las tablas
VAL_ANIO_MES_INI_PROC_DATE=`date '+%Y-%m-' -d "$VAL_FEC_INI_MES -1 month"` #Fecha inicio del Mes siguiente Formato YYYYMMDD: 20210801
VAL_FECHA_FIN_MES_PROC=`date '+%Y%m%d' -d "$VAL_FEC_INI_MES -1 day"` #Fecha fin del Mes anterior Formato YYYYMMDD: 20210831
VAL_FECHA_INI_MES_ANT_PROC=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -2 month"` #Fecha inicio del 1 Mes anterior al proceso Formato YYYYMMDD: 20210701
VAL_FECHA_INI_MES_ANT_PROC1=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -3 month"` #Fecha inicio del 2 Meses anterior al proceso Formato YYYYMMDD: 20210601
VAL_FECHA_INI_MES_ANT_PROC2=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -4 month"` #Fecha inicio del 3 Meses anterior al proceso Formato YYYYMMDD: 20210501
VAL_FECHA_INI_MES_ANT_PROC3=`date '+%Y%m01' -d "$VAL_FEC_INI_MES -5 month"` #Fecha inicio del 4 Meses anterior al proceso Formato YYYYMMDD: 20210401
VAL_LOG=$VAL_RUTA/log/OTC_T_LINEAS_ACTIVAS_$HORA.log

#SE MUESTRA LA HORA DE INICIACION DEL PROCESO Y EL PARAMETRO RECIBIDO
echo "Hora: $HORA" &>> $VAL_LOG
echo "Fecha Proceso: $VAL_FECHA_PROCESO" &>> $VAL_LOG
echo "Fecha inicio del mes de ejecucion: $VAL_FEC_INI_MES" &>> $VAL_LOG
echo "Fecha inicio del proceso: $VAL_FECHA_INI_PROC" &>> $VAL_LOG
echo "Fecha inicio del mes de proceso: $VAL_FECHA_INI_MES_PROC" &>> $VAL_LOG
echo "Anio y mes de la fecha inicio de proceso: $VAL_ANIO_MES_INI_PROC_DATE" &>> $VAL_LOG
echo "Fecha fin del mes proceso: $VAL_FECHA_FIN_MES_PROC" &>> $VAL_LOG
echo "Fecha inicio del mes anterior proceso menos 1: $VAL_FECHA_INI_MES_ANT_PROC" &>> $VAL_LOG
echo "Fecha inicio del mes anterior proceso menos 2: $VAL_FECHA_INI_MES_ANT_PROC1" &>> $VAL_LOG
echo "Fecha inicio del mes anterior proceso menos 3: $VAL_FECHA_INI_MES_ANT_PROC2" &>> $VAL_LOG
echo "Fecha inicio del mes anterior proceso menos 4: $VAL_FECHA_INI_MES_ANT_PROC3" &>> $VAL_LOG

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$VAL_FECHA_PROCESO" ] 
|| [ -z "$VAL_COLA_EJECUCION" ] 
|| [ -z "$VAL_RUTA" ] ; then
  echo "[ERROR] uno de los parametros esta vacio o nulo"&>> $VAL_LOG
  exit 1
fi
  
#PASO 3: HACE LA EJECUCION DEL PROCESO PARA EL REPORTE ARCOTEL LINEAS ACTIVAS
echo "==== Inicia ejecucion del proceso GENERACION DE REPORTE ARCOTEL LINEAS ACTIVAS  ===="`date '+%Y%m%d%H%M%S'` &>> $VAL_LOG

#SPARK DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
--driver-memory $VAL_DRIVER_MEMORY \
$VAL_RUTA/python/$VAL_PYTHON \
--ventidad=$ENTIDAD \
--vtdbhive1=$VAL_TDBHIVE1 \
--vttblhive1=$VAL_TTBLHIVE1 \
--vtdbhive2=$VAL_TDBHIVE2 \
--vttblhive2=$VAL_TTBLHIVE2 \
--vtdbhive3=$VAL_TDBHIVE3 \
--vttblhive3=$VAL_TTBLHIVE3 \
--vtdbhive4=$VAL_TDBHIVE4 \
--vttblhive4=$VAL_TTBLHIVE4 \
--vtdbhive5=$VAL_TDBHIVE5 \
--vttblhive5=$VAL_TTBLHIVE5 \
--vtdbhive6=$VAL_TDBHIVE6 \
--vttblhive6=$VAL_TTBLHIVE6 \
--vtdbhive7=$VAL_TDBHIVE7 \
--vttblhive7=$VAL_TTBLHIVE7 \
--vtdbhive8=$VAL_TDBHIVE8 \
--vttblhive8=$VAL_TTBLHIVE8 \
--vtdbhive9=$VAL_TDBHIVE9 \
--vttblhive9=$VAL_TTBLHIVE9 \
--vtdbhive10=$VAL_TDBHIVE10 \
--vttblhive10=$VAL_TTBLHIVE10 \
--vtdbhive11=$VAL_TDBHIVE11 \
--vttblhive11=$VAL_TTBLHIVE11 \
--vtdbhive12=$VAL_TDBHIVE12 \
--vttblhive12=$VAL_TTBLHIVE12 \
--vfecha_inicio_mes=$VAL_FECHA_INI_MES_PROC \
--vfecha_inicio_proceso=$VAL_FECHA_INI_PROC \
--vfec_fin_mes=$VAL_FEC_INI_MES \
--vfecha_fin_mes_act=$VAL_FECHA_FIN_MES_PROC \
--vfecha_ini_mes_ant=$VAL_FECHA_INI_MES_ANT_PROC \
--vfecha_ini_mes_ant1=$VAL_FECHA_INI_MES_ANT_PROC1 \
--vfecha_ini_mes_ant2=$VAL_FECHA_INI_MES_ANT_PROC2 \
--vfecha_ini_mes_ant3=$VAL_FECHA_INI_MES_ANT_PROC3 \
--vesquematmp=$VAL_ESQUEMA_TMP \
--vesquemarpt=$VAL_ESQUEMA_RPT &&>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - PROCESO REPORTE ARCOTEL LINEAS ACTIVAS ===="`date '+%H%M%S'` &>> $VAL_LOG
	else
	echo "==== ERROR - PROCESO REPORTE ARCOTEL LINEAS ACTIVAS ====" &>> $VAL_LOG
	exit 1
fi

echo sh -x OTC_T_LINEAS_ACTIVAS0.sh 20230105 
