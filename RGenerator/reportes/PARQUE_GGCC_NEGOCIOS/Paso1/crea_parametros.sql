--PARAMETROS PARA LA ENTIDAD PRQGGCCNEG0010
DELETE FROM params WHERE entidad='PRQGGCCNEG0010';
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','RUTA','/RGenerator/reportes/PARQUE_GGCC_NEGOCIOS/CM_SH','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','SHELL','/RGenerator/reportes/PARQUE_GGCC_NEGOCIOS/Bin/Proceso_SAS-87.sh','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PERIODICIDAD','MENSUAL','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM1_FECHA','date_format(sysdate(),''%Y%m%d'')','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM2_QUEUENAME','''reportes''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM3_ARCHIVO','''Parque_GG_CC_NEG_''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM4_HOST','''10.112.47.36''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM5_USER','''NABIFI01''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM6_PASS','''bii2022-NA10''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM7_PORT','''9667''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM8_LDIR','''/RGenerator/reportes/PARQUE_GGCC_NEGOCIOS''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','PARAM9_RDIR','''/Parque''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','ETAPA','''1''','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('PRQGGCCNEG0010','OBSERVACION','''Extraccion Reporte de Grandes Cuentas y Negocios''','0','1');

SELECT * FROM params WHERE  ENTIDAD = 'PRQGGCCNEG0010';
