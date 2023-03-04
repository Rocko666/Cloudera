#########################################################################################################################
# MODIFICACIONES													#
# FECHA				AUTOR					DESCRIPCION DEL MOTIVO				#
# 2022-12-01			Luis Fernando Llanganate		Se migra conforme a defininciones		#
#########################################################################################################################
##############
# VARIABLES  #
############## 
ENTIDAD=VENTAS_HUNTER

# PARAMETROS GENERICOS SPARK OBTENIDOS DE LA LA TABLA params 
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params  where ENTIDAD = 'SPARK_GENERICO' and PARAMETRO = 'VAL_RUTA_SPARK';"`

# PARAMETROS ESPECIFICOS DEL SHELL MIGRADO
VAL_SHELL=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SHELL1';"`
VAL_PYTHON=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON1';"`
VAL_ftp_puerto=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_puerto';"`
VAL_ftp_user=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_user';"`
VAL_ftp_hostname=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_hostname';"`
VAL_ftp_pass=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_pass';"`
VAL_COLA=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'cola_ejecucion';"`
VAL_RUTA=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ruta';"`
VAL_ARCHIVO=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ARCHIVO';"`
VAL_ftp_path_envio=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_path_envio';"`
VAL_ftp_path=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ftp_path';"`
VAL_TDBHIVE=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TDBHIVE1';"`
VAL_TTBLHIVE=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TTBLHIVE1';"`
VAL_POST_FIJO=`mysql -N  <<<"select valor from params  where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_POST_FIJO';"`


#Validacion de parametros iniciales, nulos y existencia de Rutas
if [ -z "$VAL_SHELL" ] 
|| [ -z "$VAL_PYTHON" ] 
|| [ -z "$VAL_ftp_puerto" ] 
|| [ -z "$VAL_ftp_user" ] 
|| [ -z "$VAL_ftp_hostname" ] 
|| [ -z "$VAL_ftp_pass" ] 
|| [ -z "$VAL_ftp_path_envio" ] 
|| [ -z "$VAL_ftp_path" ] 
|| [ -z "$VAL_TDBHIVE" ] 
|| [ -z "$VAL_TTBLHIVE" ] 
|| [ -z "$VAL_POST_FIJO" ] ; then
	echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
	error=1
	exit $error
fi

ini_fecha=`date '+%Y%m%d%H%M%S'` # Se conserva

VAL_LOG=${VAL_RUTA}/logs/import_distribuidores_$ini_fecha.log # Se renombra el Directorio
Ruta_fija=${VAL_RUTA}/Input/${VAL_ARCHIVO}

VAL_TTBLHIVE=$VAL_TTBLHIVE$VAL_POST_FIJO


echo "Se inicia transferencia de Archivo desde FTP a Big Data" 2>&1 &>> $VAL_LOG

#---llevar fichero desde ftp a big data9--
    ftp -n -v  << EOF
    open ${VAL_ftp_hostname} ${VAL_ftp_puerto}  
    passive
    quote USER ${VAL_ftp_user}  
    quote PASS ${VAL_ftp_pass}  
    ascii
    get ${VAL_ftp_path} ${Ruta_fija}  
EOF

echo "Se inicia proceso de carga de datos desde PySpark"  2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK $VAL_PYTHON --file=$Ruta_fija --table=$VAL_TDBHIVE.$VAL_TTBLHIVE --dropTable='SI' --sep=';' 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'SyntaxError:|pyodbc.InterfaceError:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client|not found' $VAL_LOG | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion de la importacion de distribuidores EXITOSA ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - La ejecucion de la importacion de distribuidores Fallo ====" 2>&1 &>> $VAL_LOG
	exit 1
fi
