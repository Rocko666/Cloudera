set -e
#########################################################################
# rf/ott:                                        						#
# Cliente:           Telefonica           						    	#
# Nombre_archivo:    Reporte Grandes Cuentas y Negocios Carterizado		#
# Elaborado por:     Giovanny Castillo           						#
# fecha_proceso:     YYYYMMDD                    						#
# Modificado por:     Luis Fernando Llanganate                           						#
# Fecha de creacion: 2020/06/01                  					#
# Fecha de modificacion: 2020/09/20                         						#
#########################################################################
#Parametros
###################
ini_fecha=$(date '+%Y%m%d%H%M%S')
fecha_ejecucion=$1
QUEUENAME=$2
ARCHIVO=$3
HOST=$4
USER=$5
PASS=$6
PORT=$7
RUTA=$8 
REMOTEDIR=$9 
ETAPA=${10}
ENTIDAD=PRQGGCCNEGT0010
LOCALDIR=${RUTA}/output
path_proceso=${RUTA}
#Se obtiene la fecha del ultimo dia del mes cerrado
FechaFin=$(date -d "$(date -d $fecha_ejecucion +%Y%m01) -1 days" +%Y%m%d);
#Se obtiene la fecha del primer dia del mes en curso
FechaEjecucion=$(date -d "$(date -d $fecha_ejecucion +%Y%m01)" +%Y%m%d);
#Anio
ano=$(echo $FechaFin | cut -c1-4)
#Se obtiene el mes en palabras
mes=$(echo $FechaFin | cut -c5-6)
#
log_Extraccion=${RUTA}/logs/proceso_extraccion_sas87_$ini_fecha.log
#

#VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
#$VAL_KINIT
#################################################
#ETAPA 1: EJECUCION DEL PROCESO DE MOVIMIENTO B2B
#################################################
#
###############################
#ELIMINA LOS ARCHIVOS DEL OUTPUT
###############################
if [ "$ETAPA" = "1" ]; then
reportes=`ls ${LOCALDIR} |wc -l`

if [ $reportes -ne 0 ];then
rm ${LOCALDIR}/*.*
fi
#

# Nueva llamada a pyspark LFLM 12/09/2022 recibe como ruta el nombre del archivo con el nombre .csv la fecha del ultimo dÃ­a del mes
/usr/hdp/current/spark2-client/bin/spark-submit ${RUTA}/python/Proceso_Extraccion_SAS-87.py --ruta=${RUTA}/output/${ARCHIVO}${ano}${mes}.xlsx --fechafin=$FechaFin --tabla1='db_cs_altas.otc_t_prq_glb_bi' --tabla2='db_cs_altas.otc_t_perimetro_altas_historico'  --tabla3='db_cs_altas.otc_t_ctl_planes_categoria_tarifa'  &>> $log_Extraccion


		#
		#Conversion del archivo CSV a XLSX
		#/usr/bin/python ${path_proceso}/bin/conversion_csv_xlsx.py ${path_proceso} ${ARCHIVO}${ano}${mes}.csv ${ARCHIVO}${ano}${mes}.xlsx &>> $log_Extraccion
		#
		#reporte_csv=`ls ${LOCALDIR}/${ARCHIVO}${ano}${mes}.csv |wc -l`
		#reporte_xlsx=`ls ${LOCALDIR}/${ARCHIVO}${ano}${mes}.xlsx |wc -l`
		#
		#if [ $reporte_xlsx -eq 0 ];then
		#	error=3
		#	ETAPA=1
		#	`mysql -N  <<<"update params set valor='${ETAPA}' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
		#	echo "NO existe el archivo final xlsx" &>> $log_Extraccion
		#	echo "ERROR en la ETAPA ${ETAPA}" &>> $log_Extraccion
		#	exit $error;
		#fi
		ETAPA=2
#seteo de etapa
echo "Procesado ETAPA 1" &>> $log_Extraccion
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
#
#######################################
#ETAPA 2: PROCESO FTP DEL REPORTE FINAL
#######################################
if [ "$ETAPA" = "2" ]; then
	#Ejecucion de la shell FTP
	sh -x ${RUTA}/bin/ftp_put_sas-87.sh ${ARCHIVO}${ano}${mes} ${HOST} ${USER} ${PASS} ${PORT} ${LOCALDIR} ${REMOTEDIR} ${RUTA} &>>  $log_Extraccion
	#
	error_ftp=`egrep 'ftp Error' $log_Extraccion  | wc -l`
	if [ $error_ftp -ne 0 ];then
		error=3
		ETAPA=2
		`mysql -N  <<<"update params set valor='${ETAPA}' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
		echo "ERROR en la ETAPA ${ETAPA}" &>> $log_Extraccion
		exit $error;
	fi
#seteo de etapa
echo "Procesado ETAPA 2" &>> $log_Extraccion
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
