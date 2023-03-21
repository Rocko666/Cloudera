set -e
#########################################################################
# rf/ott:                                        						#
# Cliente:           Telefonica           						    	#
# Nombre_archivo:    Reporte Grandes Cuentas y Negocios Carterizado		#
# Elaborado por:     Giovanny Castillo           						#
# fecha_proceso:     YYYYMMDD                    						#
# Modificado por:     Luis Fernando Llanganate                     		#
# Fecha de creacion: 2020/06/01                  						#
# Fecha de modificacion: 2020/09/20                         			#
#########################################################################
# 2023-01-10    Cristian Ortiz (Softconsulting)(Migracion a CLOUDERA) 
########################################################################

###################################################################################################################
# PARAMETROS INICIALES Y DE ENTRADA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametros iniciales y de entrada"
###################################################################################################################
ENTIDAD=PRQGGCCNEGT0010
ini_fecha=$(date '+%Y%m%d%H%M%S')
fecha_ejecucion=$1

if  [ -z "$ENTIDAD" ] ||
	[ -z "$fecha_ejecucion" ] ; then
	echo " ERROR: Uno de los parametros iniciales/entrada estan vacios"
	exit 1
fi

###################################################################################################################
# VALIDAR PARAMETRO VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametro del file LOG"
###################################################################################################################
VAL_RUTA=$8
VAL_LOG=${VAL_RUTA}/Log/proceso_sas87_$ini_fecha.log

if  [ -z "$VAL_RUTA" ] ||
	[ -z "$VAL_LOG" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros esta vacio o nulo [Creacion del file log]" 
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros genericos SPARK..." 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

if  [ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$VAL_KINIT" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de SPARK GENERICO es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros definidos en la tabla params..." 2>&1 &>> $VAL_LOG
###################################################################################################################

VAL_QUEUE=$2
VAL_ARCHIVO=$3
VAL_HOST=$4
VAL_USER=$5
VAL_PASS=$6
VAL_PORT=$7
VAL_REMOTEDIR=$9
ETAPA=${10}
vTPRQ='db_cs_altas.otc_t_prq_glb_bi'
vTPAH='db_cs_altas.otc_t_perimetro_altas_historico'
vTPCT='db_cs_altas.otc_t_ctl_planes_categoria_tarifa'
VAL_RUTA_PYTHON=${VAL_RUTA}/Python
VAL_LOCALDIR=${VAL_RUTA}/Output
VAL_FILE_MAIN='Proceso_Extraccion_SAS-87.py'
VAL_ETP01_MASTER='yarn'
VAL_ETP01_DRIVER_MEMORY='4G'
VAL_ETP01_EXECUTOR_MEMORY='4G'
VAL_ETP01_NUM_EXECUTORS='2'
VAL_ETP01_NUM_EXECUTORS_CORES='2'

#Los espacios en blanco de los directorios se va a colocar un conjunto de caracteres espciale ~}< 
#Debido que en Control M para los espacios en blanco lo considera como un separador de parametros 
#Se va aplicar el comando SED para remplazar ~}< por un espacio y de esta manera la shell tome correctamente el parametro
TRREMOTEDIR=`echo $VAL_REMOTEDIR|sed "s/\~}</ /g"`
REMOTEDIRFINAL=${TRREMOTEDIR}

if  [ -z "$ETAPA" ] || 
	[ -z "$VAL_ARCHIVO" ] || 
	[ -z "$vTPRQ" ] || 
	[ -z "$vTPAH" ] || 
	[ -z "$vTPCT" ] ||
	[ -z "$VAL_RUTA_PYTHON" ] || 
	[ -z "$VAL_HOST" ] || 
	[ -z "$VAL_USER" ] || 
	[ -z "$VAL_PASS" ] ||
	[ -z "$VAL_PORT" ] ||
	[ -z "$VAL_REMOTEDIR" ] ||
	[ -z "$VAL_LOCALDIR" ] ||
	[ -z "$VAL_FILE_MAIN" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$REMOTEDIRFINAL" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc uno de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG
	error=1
	exit $error
fi

echo "----------------------------------" 2>&1 &>> $VAL_LOG
echo "HOST:" $VAL_HOST 2>&1 &>> $VAL_LOG
echo "USER: " $VAL_USER 2>&1 &>> $VAL_LOG
echo "PASS: " $VAL_PASS 2>&1 &>> $VAL_LOG
echo "PORT: " $VAL_PORT 2>&1 &>> $VAL_LOG
echo "REMOTEDIR: " $REMOTEDIRFINAL 2>&1 &>> $VAL_LOG
echo "LOCALDIR: " $VAL_LOCALDIR 2>&1 &>> $VAL_LOG
echo "****** Fin Carga variables de configuracion ******" 2>&1 &>> $VAL_LOG

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################

#Se obtiene la fecha del ultimo dia del mes cerrado
FechaFin=$(date -d "$(date -d $fecha_ejecucion +%Y%m01) -1 days" +%Y%m%d);
#Se obtiene la fecha del primer dia del mes en curso
FechaEjecucion=$(date -d "$(date -d $fecha_ejecucion +%Y%m01)" +%Y%m%d);
#Anio
anio=$(echo $FechaFin | cut -c1-4)
#Se obtiene el mes en palabras
mes=$(echo $FechaFin | cut -c5-6)

if 	[ -z "$FechaFin" ] || 
	[ -z "$FechaEjecucion" ] ||
	[ -z "$anio" ] ||
	[ -z "$mes" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG
	error=1
	exit $error
fi

FILENAME=${VAL_ARCHIVO}${anio}${mes}.xlsx
echo " INFO: FechaFin => " $FechaFin 2>&1 &>> $VAL_LOG
echo " INFO: FechaEjecucion => " $FechaEjecucion 2>&1 &>> $VAL_LOG
echo " INFO: Anio => " $anio 2>&1 &>> $VAL_LOG
echo " INFO: Mes => " $mes 2>&1 &>> $VAL_LOG
echo "ARCHIVO: " $FILENAME 2>&1 &>> $VAL_LOG
#################################################
#ETAPA 1: EJECUCION DEL PROCESO DE MOVIMIENTO B2B
#################################################

###############################
#ELIMINA LOS ARCHIVOS DEL OUTPUT
###############################
if [ "$ETAPA" = "1" ]; then
reportes=`ls ${VAL_LOCALDIR} |wc -l`

if [ $reportes -ne 0 ];then
rm ${VAL_LOCALDIR}/*.*
fi

# Nueva llamada a pyspark LFLM 12/09/2022 recibe como ruta el nombre del archivo con el nombre .csv la fecha del ultimo dÃ­a del mes
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
${VAL_RUTA_PYTHON}/$VAL_FILE_MAIN \
--ruta=${VAL_LOCALDIR}/$FILENAME \
--fechafin=$FechaFin \
--tabla1=$vTPRQ \
--tabla2=$vTPAH  \
--tabla3=$vTPCT \
--queue=$VAL_QUEUE 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark $VAL_FILE_MAIN es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG
		`mysql -N  <<<"update params set valor='${ETAPA}' where ENTIDAD = 'PRQGGCCNEG0010' and parametro = 'ETAPA';"`
	else		
		echo "==== ERROR: - En la ejecucion del archivo spark $VAL_FILE_MAIN ====" 2>&1 &>> $VAL_LOG
		exit 1
	fi
fi
#
#######################################
#ETAPA 2: PROCESO FTP DEL REPORTE FINAL
#######################################
if [ "$ETAPA" = "2" ]; then
	echo "Inicio transferencia ARCHIVO: " $FILENAME

#FTP CONNECTION
ftp -inv $VAL_HOST $VAL_PORT <<EOF 2>&1 &>> $VAL_LOG
user $VAL_USER $VAL_PASS
bin
cd ${REMOTEDIRFINAL}
lcd ${VAL_LOCALDIR}
mput ${FILENAME}
bye
EOF
#
FTP_SUCCESS_MSG="226 Transfer complete"

if fgrep "$FTP_SUCCESS_MSG" $VAL_LOG ;then
	echo "==== OK - La transferencia del archivo $FILENAME a ruta FTP es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 2 con EXITO" 2>&1 &>> $VAL_LOG	
	ETAPA=1
	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	`mysql -N  <<<"update params set valor='${ETAPA}' where ENTIDAD = 'PRQGGCCNEG0010' and parametro = 'ETAPA';"`
else
	echo "Error en la transferencia del archivo $FILENAME a ruta FTP" 2>&1 &>> $VAL_LOG
	exit 3;
fi
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso PARQUE_GGCC_NEGOCIOS finaliza correctamente " 2>&1 &>> $VAL_LOG

fi
