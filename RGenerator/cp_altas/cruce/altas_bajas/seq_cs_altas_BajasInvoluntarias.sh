##########################################################################
# MODIFICACIONES														 #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO						 #
# 2022-12-27	Brigitte Balon Se migra importacion a spark              #								
##########################################################################

ENTIDAD=D_BIGCSALTBAJINV0391
fecha_eject=$1

#PARAMETROS DE LA ENTIDAD
RUTA=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
HIVEDB=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"'  AND parametro = 'HIVEDB';"`
VAL_MASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_EXECUTOR_CORES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params_des
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`

VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_USER';"`

fecha_hoy=`date '+%Y%m%d'`
ini_fecha=`date '+%Y%m%d%H%M%S'`
log_Extraccion=$RUTA/logs/extraccion_proceso_bajasInvo_$ini_fecha.log
fecha_nueva=`date -d "${fecha_hoy} -7 day"  +"%Y%m%d"`

#bba 
VAL_USER=nae106995

#Verificar demas parametros de Mysql
if [ -z "$RUTA" ] || [ -z "$VAL_MASTER" ] || [ -z "$VAL_DRIVER_MEMORY" ] || [ -z "$VAL_NUM_EXECUTORS" ] || [ -z "$VAL_RUTA_SPARK" ] || [ -z "$HIVEDB" ] || [ -z "$VAL_EXECUTOR_CORES" ]; then
    error=1
    echo " $TIME [ERROR] $rc alguno de los parametros esta vacio o nulo"
    exit $error
fi

#TABLAS DESTINO
vTabla1=$HIVEDB".otc_t_bajas_involuntarias"
vTabla2=$HIVEDB".otc_t_2tmp_bajas_involuntarias"

##bba
vTabla1=$HIVEDB".otc_t_bajas_involuntarias_prycldr"
vTabla2=$HIVEDB".otc_t_2tmp_bajas_involuntarias_prycldr"


echo "==== INICIA PROCESO SPARK ====" > $log_Extraccion
echo "***********************************************************************************" >> $log_Extraccion
echo "--------***** PROCESO BAJAS INVOLUNTARIAS *****---------" >> $log_Extraccion
echo "ESQUEMA HIVE:      "$HIVEDB >> $log_Extraccion
echo "FECHA EJECUCION:   "$fecha_eject >> $log_Extraccion
echo "FECHA ACTUAL:      "$fecha_hoy >> $log_Extraccion
echo "TABLA DESTINO:	 "$vTabla1 >> $log_Extraccion
echo "TABLA DESTINO:	 "$vTabla2 >> $log_Extraccion
echo "----------------------------------------------------------------------------------------------------" >> $log_Extraccion

$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_EXECUTOR_CORES \
$RUTA/python/proceso_bajasInvo.py \
--ventidad=$ENTIDAD \
--vhivebd=$HIVEDB \
--vfecha_ejec=$fecha_eject \
--vfecha_hoy=$fecha_hoy \
--vTabla2=$vTabla2 \
--val_cadena_jdbc=$VAL_CADENA_JDBC \
--val_user=$VAL_USER \
--vTabla1=$vTabla1 &>> $log_Extraccion

error_spark=`egrep 'error: argument|invalid syntax|An error occurred|Caused by:|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|ImportError|SyntaxError' $log_Extraccion | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark proceso_bajasInvo.py es EXITOSO ===="`date '+%H%M%S'` >> $log_Extraccion
else
echo "==== ERROR: - En la ejecucion del archivo spark proceso_bajasInvo.py ====" >> $log_Extraccion
exit 1
fi


