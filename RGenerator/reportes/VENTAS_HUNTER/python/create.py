from functools import wraps 
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index
import requests, re, sys
reload (sys)
sys.setdefaultencoding ( "utf-8")


def drop_tmp(esquema,name_tabla):
	qry="""
		DROP TABLE IF EXISTS {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_t_geographical_locations_tmp(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} As
		select g.c_geograph_loc_id as c_geograph_loc_id
		,g.C_GEO_SEGMENT_ID as C_GEO_SEGMENT_ID
		,g.c_geo_parent_location_id as c_geo_parent_location_id 
		,g.c_geograph_loc_name as c_geograph_loc_name
		,c.c_geograph_loc_name as c_geograph_loc_name2
		from db_temporales.t_geographical_locations G
		LEFT JOIN db_temporales.t_geographical_locations C ON C.c_geograph_loc_id=G.c_geo_parent_location_id
		where G.C_GEO_SEGMENT_ID = 105	
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_v_sim_trx_5d_tem2(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} As
		select A.c_min,
		A.C_SIM_SERIAL_NO,
		A.C_SIM_TRANS_ID,
		A.C_SIM_TRANS_DATE,
		A.C_MAIN_PARTNER_ID,
		A.C_FROM_PARTNER,
		A.C_PARTNER_ID,
		A.C_ACTIVATED_DATE
		from db_payment_manager.OTC_V_SIM_TRX_5D A
		inner join 
		(select C_MIN, C_MAIN_PARTNER_ID, max(c_sim_trans_date ) as c_sim_trans_date from db_payment_manager.OTC_V_SIM_TRX_5D
		where c_main_partner_id in (select MAIN_DISTRIBUITOR from db_temporales.carga_distribuidores)
		group by C_MIN, C_MAIN_PARTNER_ID) B 
		ON B.c_min = A.c_min and B.c_sim_trans_date= A.c_sim_trans_date
		where (A.c_from_partner = A.C_MAIN_PARTNER_ID or
		A.c_from_partner is null)	
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_v_sim_trx_5d_tem3(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} As
		select A.c_min,
		A.C_SIM_SERIAL_NO,
		A.C_SIM_TRANS_ID,
		A.C_SIM_TRANS_DATE,
		A.C_MAIN_PARTNER_ID,
		A.C_FROM_PARTNER,
		A.C_PARTNER_ID,
		A.C_ACTIVATED_DATE
		from db_temporales.OTC_V_SIM_TRX_5D_tem2 A
		inner join 
		(select C_MIN, max(C_SIM_TRANS_ID) as C_SIM_TRANS_ID from db_temporales.OTC_V_SIM_TRX_5D_tem2
		group by C_MIN) B 
		ON B.c_min = A.c_min and B.C_SIM_TRANS_ID= A.C_SIM_TRANS_ID
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_t_ventas_hunter_tmp(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} (
		PARTNER_ID String,
		MAIN_PARTNER_ID String,
		PARTNER_PARENT_ID	 String,
		nombre_compania String,
		ciudad String,
		C_MIN int,
		c_sim_trans_date int,
		C_ACTIVATED_DATE int,
		C_PAYMENT_AMOUNT int,
		fecha_proceso int
		)
		STORED as ORC tblproperties ("orc.compress" = "SNAPPY")
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_t_inventario_hunter_tmp(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} (
		MAIN_PARTNER_ID String,
		PARTNER_PARENT_ID	 String,
		PARTNER_ID String,
		Entregadas int,
		Activadas int,
		Remanente int
		)
		STORED as ORC tblproperties ("orc.compress" = "SNAPPY")
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_t_conteo_ventas(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} AS
		select A.partner_id, 
		A.main_partner_id,
		A.c_payment_amount,
		count(A.c_activated_date) as conteo,
		B.celular as Celular
		from db_temporales.otc_t_Ventas_Hunter_TMP A
		left join db_temporales.carga_distribuidores B on A.partner_id = trim(B.codigo_payment) 
		where A.main_partner_id in (select MAIN_DISTRIBUITOR from db_temporales.carga_distribuidores)
		group by partner_id , A.main_partner_id, c_payment_amount, Celular	
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def create_otc_t_conteo_ventas_dia(esquema,name_tabla,fecha_proceso):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} AS
		select A.partner_id, 
		A.main_partner_id,
		A.c_payment_amount,
		count(A.c_activated_date) as conteo,
		B.celular as Celular
		from db_temporales.otc_t_Ventas_Hunter_TMP A
		left join db_temporales.carga_distribuidores B on A.partner_id = trim(B.codigo_payment) 
		where A.main_partner_id in (select MAIN_DISTRIBUITOR from db_temporales.carga_distribuidores)
		and A.c_activated_date = {fecha_proceso} 
		group by partner_id , A.main_partner_id, c_payment_amount, Celular	
	""".format(esquema=esquema,name_tabla=name_tabla,fecha_proceso=fecha_proceso)
	return qry

def create_otc_t_ventas_hunter_acumuladas(esquema,name_tabla,fecha_proceso):
	qry="""
	CREATE TABLE {esquema}.{name_tabla} AS
	SELECT 
	'A1000' as  Tipo_Registro,
	concat('OTECEL\\',partner_id) AS Cod_Cliente,
	'SIN NOMBRE'as Nom_cliente,
	'Masivo' as Categoria,
	'SMS' as Idenficador_de_campana,
	concat(593,CELULAR) as MSISDN,
	concat('Venta Acumulada al ? CHIP$0 ',CASE WHEN COLLECT_SET(Combo0)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo0)[0] END,
	',CHIP$3 ',CASE WHEN COLLECT_SET(Combo3)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo3)[0] END,
	',CHIP$5 ',CASE WHEN COLLECT_SET(Combo5)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo5)[0] END,
	',CHIP$7 ',CASE WHEN COLLECT_SET(Combo7)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo7)[0] END,
	',CHIP$10 ',CASE WHEN COLLECT_SET(Combo10)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo10)[0] END) AS Mensajes
	FROM ( SELECT partner_id, CELULAR, 
	CASE WHEN c_payment_amount is null THEN conteo END AS Combo0,
	CASE WHEN c_payment_amount=3 THEN conteo END AS Combo3,
	CASE WHEN c_payment_amount=5 THEN conteo END AS Combo5,
	CASE WHEN c_payment_amount=7 THEN conteo END AS Combo7,
	CASE WHEN c_payment_amount=10 THEN conteo END AS Combo10
	FROM db_temporales.otc_t_conteo_ventas) A
	where celular is not null
	GROUP BY partner_id ,celular	
	""".format(esquema=esquema,name_tabla=name_tabla,fecha_proceso=fecha_proceso)
	return qry

def create_otc_t_ventas_hunter_acumuladas_reporte(esquema,name_tabla):
	qry="""
		CREATE TABLE {esquema}.{name_tabla} AS
		SELECT main_partner_id,
		partner_id as Cod_Cliente, 
		CELULAR as MSISDN,
		CASE WHEN COLLECT_SET(Combo0)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo0)[0] END AS Combo0,
		CASE WHEN COLLECT_SET(Combo3)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo3)[0] END AS Combo3,
		CASE WHEN COLLECT_SET(Combo5)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo5)[0] END AS Combo5,
		CASE WHEN COLLECT_SET(Combo7)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo7)[0] END AS Combo7, 
		CASE WHEN COLLECT_SET(Combo10)[0] IS NULL THEN 0 ELSE COLLECT_SET(Combo10)[0] END AS Combo10 
		FROM ( SELECT partner_id, CELULAR, main_partner_id,
		CASE WHEN c_payment_amount is null THEN conteo END AS Combo0,
		CASE WHEN c_payment_amount=3 THEN conteo END AS Combo3,
		CASE WHEN c_payment_amount=5 THEN conteo END AS Combo5,
		CASE WHEN c_payment_amount=7 THEN conteo END AS Combo7,
		CASE WHEN c_payment_amount=10 THEN conteo END AS Combo10
		FROM db_temporales.otc_t_conteo_ventas) A
		GROUP BY partner_id, CELULAR, main_partner_id
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

