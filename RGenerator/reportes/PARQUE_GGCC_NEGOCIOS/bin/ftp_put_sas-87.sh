#!/bin/bash
#########################################################################
# Nombre de la empresa.........: INDRA	                                #
# Fecha creacion original......: 01 de Junio de 2020                    #
# Objetivo.....................: Shell que deposita reporte de Grandes Cuentas #
#                                Negocios                               #
#########################################################################
#
ini_fecha=`date '+%Y%m%d%H%M%S'`
#
##############
#PARAMETROS
##############
echo "****** Carga variables de configuracion ******"
ARCHIVO_RF=$1.xlsx
HOST=$2
USER=$3
PASS=$4
PORT=$5
LOCALDIR=$6
REMOTEDIR=$7
RUTA=$8
#
log_Proceso=${RUTA}/logs/ftp_put_sas-87_$ini_fecha.log	
#
#Los espacios en blanco de los directorios se va a colocar un conjunto de caracteres espciale ~}< 
#Debido que en Control M para los espacios en blanco lo considera como un separador de parametros 
#Se va aplicar el comando SED para remplazar ~}< por un espacio y de esta manera la shell tome correctamente el parametro
#
TRREMOTEDIR=`echo $REMOTEDIR|sed "s/\~}</ /g"`
REMOTEDIRFINAL=${TRREMOTEDIR}
#
#
	echo "----------------------------------" > $log_Proceso
	echo "HOST:" $HOST >> $log_Proceso
	echo "USER: " $USER >> $log_Proceso
	echo "PASS: " $PASS >> $log_Proceso
	echo "PORT: " $PORT >> $log_Proceso
	echo "REMOTEDIR: " $REMOTEDIRFINAL >> $log_Proceso
	echo "LOCALDIR: " $LOCALDIR >> $log_Proceso
	echo "ARCHIVO: " $ARCHIVO_RF >> $log_Proceso
	
	echo "****** Fin Carga variables de configuracion ******"
	
#Nombre del archivo generado en SAS-87
FILENAME=${ARCHIVO_RF}
echo "ARCHIVO: " $FILENAME
##################################
#Realiza el proceso de EjecuciÃ³n #
##################################

#FTP CONNECTION

ftp -inv $HOST $PORT <<EOF >> $log_Proceso
user $USER $PASS

bin

cd ${REMOTEDIRFINAL}

lcd ${LOCALDIR}
mput ${FILENAME}

bye
EOF
#
FTP_SUCCESS_MSG="226 Transfer complete"
if fgrep "$FTP_SUCCESS_MSG" $log_Proceso ;then
      echo "ftp OK" >> $log_Proceso
else
      echo "ftp Error" >> $log_Proceso
	  exit 3;
fi


