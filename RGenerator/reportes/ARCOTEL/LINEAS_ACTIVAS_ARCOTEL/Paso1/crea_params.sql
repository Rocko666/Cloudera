
DELETE FROM params WHERE ENTIDAD = 'LNSACTARCTL0010' ;
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_RUTA','/RGenerator/reportes/VENTAS_HUNTER','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_SHELL','OTC_T_LINEAS_ACTIVAS.sh','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_PYTHON','otc_t_lineas_activas.py','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_COLA','capa_semantica','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE1','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE1','tmp_traf_arcotel_completo','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE2','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE2','tmp_traf_arcotel','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE3','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE3','tmp_parque_tecno_datos','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE4','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE4','tmp_traf_arcotel_voz_completo','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE5','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE5','tmp_traf_arcotel_voz','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE6','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE6','tmp_parque_tecno_voz_dat','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE7','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE7','tmp_parque_tecno_voz_dat_360','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE8','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE8','tmp_parque_tecno_voz_dat_his','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE9','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE9','otc_t_tec_lineas_activas_detalles','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE10','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE10','otc_t_tec_lineas_activas','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE11','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE11','otc_t_tec_lineas_activas_voz','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TDBHIVE12','db_desarrollo2021','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TTBLHIVE12','otc_t_tec_lineas_activas_datos','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_POST_FIJO','_prycldr','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_ESQUEMA_TMP','_tmprl','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_ESQUEMA_RPT','_rprts','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_MASTER','yarn','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_DRIVER_MEMORY','2G','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_EXECUTOR_MEMORY','4G','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_EXECUTOR_CORES','4','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_NUM_EXECUTORS','10','0','0');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('LNSACTARCTL0010','VAL_TIPO_CARGA','append','0','0');

SELECT * FROM params WHERE ENTIDAD = 'LNSACTARCTL0010';