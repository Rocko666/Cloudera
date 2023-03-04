from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index

def exp_function(i,esquema,name_tabla) :
	if (i==1) : return export_oracle_otc_v_partner_sim(esquema,name_tabla)
	if (i==2) : return export_oracle_otc_v_settrans_pm_pkt(esquema,name_tabla)


def export_oracle_otc_v_sim_trx_5d(esquema,name_tabla,fecha_inicio):
	qry="""
	(Select 
	c_min, 
	c_sim_serial_no, 
	c_sim_trans_id, 
	c_main_partner_id, 
	c_from_partner, 
	c_partner_id, 
	c_activated_date, 
	to_number(to_char(c_sim_trans_date , 'yyyymmdd')) as c_sim_trans_date 
	From {esquema}.{name_tabla}
	where to_number(to_char(c_sim_trans_date , 'yyyymmdd')) >=  to_number('{fecha_inicio}'))
	""".format(esquema=esquema,name_tabla=name_tabla,fecha_inicio=fecha_inicio)
	return qry

def export_oracle_otc_v_partner_sim(esquema,name_tabla) :
	qry="""
	(
	Select 
	c_partner_id, 
	c_partner_parent_id,
	c_partner_type_id, 
	c_sub_partner_id, 
	c_partner_company_name, 
	c_partner_contact_firstname, 
	c_partner_contact_lastname, 
	c_address_id, 
	c_payment_type_id, 
	c_billing_cycle_id, 
	c_admin_user, 
	c_last_update_date, 
	c_last_updated_by, 
	c_creation_date, 
	c_partner_login_id, 
	c_partner_password, 
	c_last_update_comments, 
	c_partner_email_id, 
	c_bill_address_id, 
	c_location_id, 
	c_gl_code, 
	c_distributor_type_id, 
	c_status, 
	c_commission_flag, 
	c_accnt_bal, 
	c_account_bal_id, 
	c_ruc_number, 
	c_ivr_transfer_no, 
	c_max_fund_limit, 
	c_nz_threshold_limit_count, 
	c_scl_client_code, 
	c_salesperson_mdn, 
	c_password_update_date, 
	c_origin_code, 
	c_origin_start_date, 
	c_origin_expiry_date, 
	c_use_own_funds, 
	c_lateral_funds, 
	c_is_recoup, 
	c_geograph_loc_id, 
	c_profile_id, 
	c_business_classification, 
	c_main_partner_id, 
	c_hierarchy_level, 
	c_segment_code, 
	c_last_segmentation_date, 
	c_relationship_type, 
	c_is_auto_retired, 
	c_retired_date, 
	c_sim_threshold, 
	c_is_sales_force, 
	c_sim_trans_enabled, 
	c_zone_id, 
	c_currency_id, 
	c_is_single_fund_source 
	From {esquema}.{name_tabla})
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def export_oracle_otc_v_settrans_pm_pkt(esquema,name_tabla) :
	qry="""
	(
	select 
	c_transaction_datetime, 
	partner_id, 
	c_packet_code, 
	c_payment_amount, 
	c_customer_id, 
	c_transaction_id, 
	c_main_partner_id, 
	operadora, 
	c_canal_id, 
	c_canal 
	From {esquema}.{name_tabla})
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def OTC_V_SIM_TRX_5D_TEM3():
	qry="""
	select * from db_temporales.OTC_V_SIM_TRX_5D_TEM3
	"""
	return qry

def error(esquema,name_tabla,fecha_reporte):
	qry="""
	select DISTINCT telefono, fecha_alta from {esquema}.{name_tabla} where p_fecha_proceso = {fecha_reporte} LIMIT 100
	""".format(esquema=esquema,name_tabla=name_tabla,fecha_reporte=fecha_reporte)
	return qry

def OTC_V_SETTRANS_PM_PKT():
	qry="""
	select * from db_temporales.OTC_V_SETTRANS_PM_PKT
	"""
	return qry

def otc_t_ventas_hunter_acumuladas():
	qry="""
	SELECT partner_id, CELULAR, 
	CASE WHEN c_payment_amount is null THEN conteo END AS Combo0,
	CASE WHEN c_payment_amount=3 THEN conteo END AS Combo3,
	CASE WHEN c_payment_amount=5 THEN conteo END AS Combo5,
	CASE WHEN c_payment_amount=7 THEN conteo END AS Combo7,
	CASE WHEN c_payment_amount=10 THEN conteo END AS Combo10
	FROM db_temporales.otc_t_conteo_ventas
	"""
	return qry

def t_geographical_locations_TMP(esquema,name_tabla):
	qry="""
	Select * From From {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def otc_t_Ventas_hunter_acumuladas_reporte(esquema,name_tabla):
	qry="""
	Select 
	main_partner_id,
	cod_cliente,
	msisdn,
	combo0,
	combo3,
	combo5,
	combo7,
	combo10
	From {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def otc_t_ventas_hunter_tmp(fecha_proceso,fecha_reporte):
	qry="""
	Select
	P.C_PARTNER_ID as PARTNER_ID
	,P.C_MAIN_PARTNER_ID as MAIN_PARTNER_ID
	,P.C_PARTNER_PARENT_ID as PARTNER_PARENT_ID
	,P.C_PARTNER_COMPANY_NAME as nombre_compania
	,C.c_geograph_loc_name2 AS CIUDAD
	,B.C_MIN as C_MIN
	,B.c_sim_trans_date as c_sim_trans_date
	,cast(cast(regexp_replace(cast(D.fecha_alta as string), '-', '') as varchar(8)) as int) as C_ACTIVATED_DATE
	,E.C_PAYMENT_AMOUNT as C_PAYMENT_AMOUNT
	,{fecha_proceso} as fecha_proceso
	FROM db_temporales.OTC_V_PARTNER_SIM  P
	LEFT JOIN db_temporales.t_geographical_locations_TMP C ON C.c_geograph_loc_id=P.C_GEOGRAPH_LOC_ID
	LEFT JOIN (select * from db_temporales.OTC_V_SIM_TRX_5D_TEM3) B ON P.C_PARTNER_ID=B.C_PARTNER_ID
	LEFT JOIN (select DISTINCT telefono, fecha_alta from db_cs_altas.otc_t_altas_BI where p_fecha_proceso = {fecha_reporte}) D 
	on D.telefono =cast (B.c_min as int) 
	LEFT JOIN db_temporales.OTC_V_SETTRANS_PM_PKT E ON cast(E.C_CUSTOMER_ID as int)=D.telefono 
	and cast(from_unixtime(unix_timestamp(C_TRANSACTION_DATETIME,'yyyy-MM-dd'),'yyyyMMdd') as int)=cast(from_unixtime(unix_timestamp(D.fecha_alta,'yyyy-MM-dd'),'yyyyMMdd') as int)
	where P.C_MAIN_PARTNER_ID in (select MAIN_DISTRIBUITOR from db_temporales.carga_distribuidores)	
	""".format(fecha_proceso=fecha_proceso,fecha_reporte=fecha_reporte)
	return qry

def otc_t_inventario_hunter_tmp():
	qry="""
	select 
	MAIN_PARTNER_ID,
	PARTNER_PARENT_ID, 
	PARTNER_ID,
	count (c_min) as Entregadas,
	count (C_ACTIVATED_DATE) as Activadas,
	(count (c_min) - count (C_ACTIVATED_DATE)) as Remanente
	from db_temporales.otc_t_Ventas_Hunter_TMP
	where c_min is not null
	group by MAIN_PARTNER_ID, PARTNER_PARENT_ID, PARTNER_ID
	order by MAIN_PARTNER_ID, PARTNER_PARENT_ID, PARTNER_ID
	"""
	return qry

def t_geographical_locations_TMP(esquema,name_tabla):
	qry="""
	SELECT 
	c_geograph_loc_id,
	c_geo_segment_id,
	c_geo_parent_location_id,
	c_geograph_loc_name,
	c_geograph_loc_name2
	From  {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def OTC_V_SIM_TRX_5D_TEM2(esquema,name_tabla):
	qry="""
	SELECT 
	c_min
	c_sim_serial_no,
	c_sim_trans_id,
	c_sim_trans_date,
	c_main_partner_id,
	c_from_partner,
	c_partner_id,
	c_activated_date
	From  {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def OTC_V_SIM_TRX_5D_TEM3(esquema,name_tabla):
	qry="""
	SELECT 
	c_min,
	c_sim_serial_no,
	c_sim_trans_id,
	c_sim_trans_date,
	c_main_partner_id,
	c_from_partner,
	c_partner_id,
	c_activated_date
	From  {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def otc_t_conteo_ventas(esquema,name_tabla):
	qry="""
	SELECT 
	partner_id,
	main_partner_id,
	c_payment_amount,
	conteo,
	celular
	From  {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry

def otc_t_conteo_ventas_dia(esquema,name_tabla):
	qry="""
	SELECT 
	partner_id,
	main_partner_id,
	c_payment_amount,
	conteo,
	celular
	From  {esquema}.{name_tabla}
	""".format(esquema=esquema,name_tabla=name_tabla)
	return qry


