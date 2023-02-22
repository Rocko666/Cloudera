#!/bin/bash
#########################################################################################################
# NOMBRE: extraccion_fact_accotcattrdetails.sh	     	      								            #
# DESCRIPCION:																							#
#  Shell que realiza el proceso de importacion la tabla accotcattrdetails de Oracle a la tabla			#
#  otc_t_accotcattrdetails en Hive																		#
# AUTOR: Karina Castro - Softconsulting                            										#
# FECHA CREACION: 2022-12-01   																			#
# PARAMETROS DEL SHELL                            													    #
# N/A																									#						
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
#########################################################################################################
ENTIDAD=OTC_T_ACCOTCATTRDETAILS

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`

#
TDDB_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDDB_RBM';"`
TDHOST_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDHOST_RBM';"`
TDPASS_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO'  AND parametro = 'TDPASS_RBM';"`
TDPORT_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO'  AND parametro = 'TDPORT_RBM';"`
TDSERVICE_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO'  AND parametro = 'TDSERVICE_RBM';"`
TDUSER_RBM=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO'  AND parametro = 'TDUSER_RBM';"`


#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@$TDHOST_RBM:$TDPORT_RBM/$TDSERVICE_RBM
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/logs/OTC_T_ACCOTCATTRDETAILS_$VAL_DIA$VAL_HORA.log

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] || 
    [ -z "$VAL_KINIT" ] || 
    [ -z "$VAL_RUTA_SPARK" ] || 
    [ -z "$VAL_RUTA_LIB" ] || 
    [ -z "$VAL_RUTA_IMP_SPARK" ] || 
    [ -z "$VAL_NOM_IMP_SPARK" ] || 
    [ -z "$VAL_NOM_JAR_ORC_11" ] || 
    [ -z "$TDDB_RBM" ] || 
    [ -z "$TDHOST_RBM" ] || 
    [ -z "$TDPASS_RBM" ] || 
    [ -z "$TDPORT_RBM" ] || 
    [ -z "$TDSERVICE_RBM" ] || 
    [ -z "$TDUSER_RBM" ] || 
    [ -z "$VAL_RUTA" ] || 
    [ -z "$HIVEDB" ] || 
    [ -z "$HIVETABLE" ] || 
    [ -z "$VAL_TIPO_CARGA" ] || 
    [ -z "$VAL_MASTER" ] || 
    [ -z "$VAL_DRIVER_MEMORY" ] || 
    [ -z "$VAL_EXECUTOR_MEMORY" ] || 
    [ -z "$VAL_NUM_EXECUTORS" ] || 
    [ -z "$VAL_JDBCURL" ] || 
    [ -z "$VAL_LOG" ]; then
	echo " ERROR: - uno de los parametros esta vacio o nulo"
	exit 1
fi

echo "==== Inicia ejecucion del proceso OTC_T_ACCOTCATTRDETAILS ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Los parametros del proceso son los siguientes:" >> $VAL_LOG
echo "Conexion a Oracle: $VAL_JDBCURL" >> $VAL_LOG

#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name OTC_T_ACCOTCATTRDETAILS \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$TDDB_RBM \
--vclavebd=$TDPASS_RBM \
--vhivebd=$HIVEDB \
--vtablahive=$HIVETABLE \
--vtipocarga=$VAL_TIPO_CARGA \
--vfilesql=$VAL_RUTA/sql/otc_t_accotcattrdetails.sql &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
echo "==== OK - La ejecucion del archivo spark spark_import2.py es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else
echo "==== ERROR: - En la ejecucion del archivo spark spark_import2.py ====" >> $VAL_LOG
exit 1
fi

echo "==== Finaliza ejecucion del proceso OTC_T_ACCOTCATTRDETAILS ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG





