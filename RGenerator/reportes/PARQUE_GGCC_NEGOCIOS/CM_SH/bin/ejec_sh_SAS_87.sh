#!/bin/bash
##########################################################################
# SCRIPT DE CARGA AUTOMATICA PARA EL PROCESO PARQUE ROAMING              #
# Creado 01-JUN-2020 (NE) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#

#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
	echo "NOMBRE_JOB_CM: " $NOMBRE_JOB_CM
ENTIDAD=$NOMBRE_JOB_CM
#ENTIDAD=PRQGGCCNEG0010
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=`date '+%Y%m%d'` 

#*********************************************************************************#
#                                                  Ã‚Â¡Ã‚Â¡ ATENCION !!                                   #
#                                                                                 #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params     #
# en el servidor 10.112.152.183                                                   #
#*********************************************************************************#

#Parametros Iniciales
		RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA');"`
		NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
		FECHA_PROCESO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PARAM1');"`

        isnum() { awk -v a="$1" 'BEGIN {print (a == a + 0)}'; }

        #Verificar que la configuraciÃƒÂ³n de la entidad exista
        if [ "$AMBIENTE" = "1" ]; then
                ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"`
        else
                ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"`
        fi

    if ! [ "$ExisteEntidad" -gt 0 ]; then #-gt mayor a -lt menor a
       echo " $TIME [ERROR] $rc No existen parametros para la entidad $ENTIDAD"
        ((rc=1))
        exit $rc
    fi

        # Verificacion de fecha de ejecucion
    if [ -z "$FECHAEJE" ]; then #valida que este en blanco el parametro
        ((rc=2))
        echo " $TIME [ERROR] $rc Falta el parametro de fecha de ejecucion del programa"
        exit $rc
    fi

        #Verificar si tuvo datos de la base
    TIME=`date +%a" "%d"/"%m"/"%Y" "%X`

    if [[ -z "$RUTA" || -z "$NAME_SHELL" ]]; then
                ((rc=3))
                echo " $TIME [ERROR] $rc No se han obtenido los valores necesarios desde la base de datos"
                exit $rc
    fi


#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------

    #DIA: Obtiene la fecha del sistema
    DIA=`date '+%Y%m%d'`
    #HORA: Obtiene hora del sistema
    HORA=`date '+%H%M%S'`
    # rc es una variable que devuelve el codigo de error de ejecucion
    ((rc=0))
    #LOGS es la ruta de carpeta de logs por entidad
    LOGS=$RUTA/Log/$ENTIDAD$FECHAEJE
        #LOGPATH ruta base donde se guardan los logs
    LOGPATH=$RUTA/Log/
	EJECUCION=$ENTIDAD$DIA$HORA
ETAPA
#------------------------------------------------------
# DEFINICION DE FUNCIONES
#------------------------------------------------------

    # Guarda los resultados en los archivos de correspondientes y registra las entradas en la base de datos de control
    function log() #funcion 4 argumentos (tipo, tarea, salida, mensaje)
    {
        if [ "$#" -lt 4 ]; then
            echo "Faltan argumentosen el llamado a la funcion"
            return 1 # Numero de argumentos no completo
        else
            if [ "$1" = 'e' -o "$1" = 'E' ]; then
                TIPOLOG=ERROR
            else
                TIPOLOG=INFO
            fi
                TAREA="$2"
                MEN="$4"
                PASO_EJEC="$5"
                FECHA=`date +%Y"-"%m"-"%d`
                HORAS=`date +%H":"%M":"%S`
                TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
                MSJ=$(echo " $TIME [$TIPOLOG] Tarea: $TAREA - $MEN ")
                echo $MSJ >> $LOGS/$EJECUCION.log
                mysql -e "insert into logs values ('$ENTIDAD','$EJECUCION','$TIPOLOG','$FECHA','$HORAS','$TAREA',$3,'$MEN','$PASO_EJEC','$NAME_SHELL')"
                echo $MSJ
                return 0
        fi
    }


    function stat() #funcion 4 argumentos (Tarea, duracion, fuente, destino)
    {
        if [ "$#" -lt 4 ]; then
            echo "Faltan argumentosen el llamado a la funcion"
            return 1 # Numero de argumentos no completo
        else
                TAREA="$1"
                DURACION="$2"
                FECHA=`date +%Y"-"%m"-"%d`
                HORAS=`date +%H":"%M":"%S`
                TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
                MSJ=$(echo " $TIME [INFO] Tarea: $TAREA - Duracion : $DURACION ")
                echo $MSJ >> $LOGS/$EJECUCION.log
                mysql -e "insert into stats values ('$ENTIDAD','$EJECUCION','$TAREA','$FECHA $HORAS','$DURACION',$3,'$4')"
                echo $MSJ
                return 0
        fi
    }
	
	function Param_Ini() #funcion 4 argumentos (Tarea, duracion, fuente, destino)
    {
        if [ "$#" -lt 1 ]; then
            echo "Faltan argumentosen el llamado a la funcion"
			((rc=3))
			log e "Ejecucion" $rc  " Faltan argumentosen el llamado a la funcion Param_Ini() "
            return $rc # Numero de argumentos no completo
        else
				JOBCTM="$1"
				VALOR_PARAM=`mysql -N  <<<"select group_concat(valor separator ',') parametros from params where ENTIDAD = '"$JOBCTM"' and (parametro like 'PARAM%' or parametro='ETAPA') group by ENTIDAD;"`
				PARAMETROS=(`mysql -N  <<<"select $VALOR_PARAM ;"`)
				#mysql -e "update params set valor = $PARAM1_INI where entidad = '$ENTIDAD' and parametro='PARAM1'; "
                echo ${PARAMETROS[*]}
        fi
    }
#------------------------------------------------------
# VERIFICACION INICIAL
#------------------------------------------------------

        #Verificar si existe la ruta de sistema
        if ! [ -e "$RUTA" ]; then
                ((rc=10))
                echo "$TIME [ERROR] $rc la ruta provista en el script no existe en el sistema o no tiene permisos sobre la misma. Cree la ruta con los permisos adecuados y vuelva a ejecutar el programa"
                exit $rc
        else
                if ! [ -e "$LOGPATH" ]; then
                        mkdir -p $LOGPATH
                                if ! [ $? -eq 0 ]; then
                                        ((rc=11))
                                        echo " $TIME [ERROR] $rc no se pudo crear la ruta de logs"
                                        exit $rc
                                fi
                fi
        fi

#------------------------------------------------------
# CREACION DE LOGS Y RUTAS NECESARIAS
#------------------------------------------------------
        echo $DIA-$HORA" Creacion de directorio para almacenamiento de logs"

        #Si ya existe la ruta en la que voy a trabajar, eliminarla
        if  [ -e "$LOGS" ]; then
            #eliminar el directorio LOGS si existiese
            #rm -rf $LOGS
                        echo $DIA-$HORA" Directorio "$LOGS " ya existe"
                else
                        #Cree el directorio LOGS para la ubicacion ingresada
                        mkdir -p $LOGS
                        #Validacion de greacion completa
            if  ! [ -e "$LOGS" ]; then
            (( rc = 21))
            echo $DIA-$HORA" Error $rc : La ruta $LOGS no pudo ser creada"
                        log e "CREAR DIRECTORIO LOG" $rc  " $DIA-$HORA' Error $rc: La ruta $LOGS no pudo ser creada'" 
            exit $rc
            fi
        fi

                 log i "PREPARACION" 0  " Creacion de directorios necesarios para la ejecucion"

        # CREACION DEL ARCHIVO DE LOG
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION.log
        if [ $? -eq 0 ]; then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION.log
        else
            (( rc = 22))
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION.log"
                        log e "CREAR ARCHIVO LOG" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de log $LOGS/$EJECUCION.log'" 
            exit $rc
        fi

        # CREACION DE ARCHIVO DE ERROR

        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION.log
        if [ $? -eq 0 ];        then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION.log
        else
            (( rc = 23))
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de error $LOGS/$EJECUCION.log"
                        log e "CREAR ARCHIVO LOG ERROR" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de error $LOGS/$EJECUCION.log'" 
            exit $rc
        fi

#------------------------------------------------------
# EJECUCION DE LA SHELL DEL PROCESO
#------------------------------------------------------
	Param_Ini $ENTIDAD
	comando="$NAME_SHELL ${PARAMETROS[*]}"
	
	sh -x $comando
	COD_EJECUCION=$?
	echo $COD_EJECUCION
	
	if [ ${COD_EJECUCION} -gt 0 ]; then
		(( rc = 3))
		log e "Ejecucion" $rc  " No se pudo ejecutar correctamente el job sh -x $comando"
		exit $rc
	else
		log i "Ejecucion" 0  " Ejecucion Exitosa del job sh -x $comando "
		rc=0 
		exit $rc
	fi
