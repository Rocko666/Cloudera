# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index

##
########################################## PROCESO ALTAS BAJAS INVOLUNTARIAS ########################################################

def part_otc_t_churn_sp2():
    qry="""
    show partitions db_cs_altas.otc_t_churn_sp2
    """
    print(qry)
    return qry

def otc_t_bajas_involuntarias(vfecha_hoy,vdiaf_mesant,vfecha_proceso):
    qry="""
    SELECT
    A.phone_id as num_telefonico
    ,cast(concat(substr(A.proces_date,1,4), '-', substr(A.proces_date,5,2), '-', substr(A.proces_date,7,2) ) as date) as fecha_proceso
    ,'{vfecha_hoy}' as fecha_ejecucion
    ,A.marca
    ,cast(A.proces_date as INT) as proces_date
    FROM db_cs_altas.OTC_T_CHURN_SP2 A inner join 
    (
    SELECT sub.phone_id
    FROM db_cs_altas.OTC_T_CHURN_SP2 sub
    WHERE sub.counted_days <= 90 
    and sub.proces_date = {vdiaf_mesant}
    ) parque on A.phone_id=parque.phone_id
    WHERE
    A.counted_days > 90
    and A.proces_date = {vfecha_proceso}
    """.format(vfecha_hoy=vfecha_hoy,vdiaf_mesant=vdiaf_mesant,vfecha_proceso=vfecha_proceso)
    print(qry)
    return qry

def otc_t_bajas_involuntarias_tot(vTablaBajInv,vfecha_proceso):
    qry="""
    SELECT 
    num_telefonico,
    fecha_proceso,
    fecha_ejecucion,
    marca,
    proces_date
    FROM {vTablaBajInv}
    where proces_date ={vfecha_proceso} 
    """.format(vTablaBajInv=vTablaBajInv,vfecha_proceso=vfecha_proceso)
    print(qry)
    return qry

def del_otc_t_bajas_involuntarias(vTablaBajInv,vfecha_proceso,vdia1_mes):
    qry="""
    SELECT 
    num_telefonico,
    proces_date
    FROM {vTablaBajInv}
    where proces_date ={vfecha_proceso} and num_telefonico in (
    SELECT A.num_telefonico
    FROM {vTablaBajInv} A
    where A.proces_date >= {vdia1_mes} and A.proces_date < {vfecha_proceso}) 
    """.format(vTablaBajInv=vTablaBajInv,vfecha_proceso=vfecha_proceso,vdia1_mes=vdia1_mes)
    print(qry)
    return qry

def otc_t_2tmp_bajas_involuntarias(vTablaBajInv,vfecha_proceso,vdia1_mes):
    qry="""
    select a.num_telefonico,a.proces_date
    FROM {vTablaBajInv} a
    LEFT OUTER JOIN (select b.phone_id from db_cs_altas.OTC_T_CHURN_SP2 b
    where b.proces_date = {vfecha_proceso})b
    ON a.num_telefonico = b.phone_id
    WHERE a.proces_date >= {vdia1_mes} and a.proces_date < {vfecha_proceso} 
    AND b.phone_id IS NULL
    """.format(vTablaBajInv=vTablaBajInv,vdia1_mes=vdia1_mes,vfecha_proceso=vfecha_proceso)
    print(qry)
    return qry

def otc_t_bajas_involuntarias_mes(vTablaBajInv,vfecha_proceso,vdia1_mes):
    qry="""
    SELECT a.num_telefonico,
    a.fecha_proceso,
    a.fecha_ejecucion,
    a.marca,
    a.proces_date 
    FROM {vTablaBajInv} a
    WHERE a.proces_date >= {vdia1_mes} and a.proces_date < {vfecha_proceso} 
    """.format(vTablaBajInv=vTablaBajInv,vfecha_proceso=vfecha_proceso,vdia1_mes=vdia1_mes)
    return qry


def del_otc_t_bajas_involuntarias2(vTablaBajInv,vfecha_proceso,vdia1_mes,vTablaBajaInvTmp):
    qry="""
    SELECT num_telefonico,
    proces_date 
    FROM {vTablaBajInv}
    where proces_date >= {vdia1_mes} and proces_date < {vfecha_proceso} 
    and num_telefonico in (SELECT num_telefonico FROM {vTablaBajaInvTmp})
    """.format(vTablaBajInv=vTablaBajInv,vfecha_proceso=vfecha_proceso,vdia1_mes=vdia1_mes,vTablaBajaInvTmp=vTablaBajaInvTmp)
    print(qry)
    return qry

def del_otc_t_bajas_involuntarias3(vTablaBajInv,vfecha_proceso,vdia1_mes):
    qry="""
    SELECT num_telefonico,
    proces_date 
    FROM {vTablaBajInv}
    WHERE proces_date >= {vdia1_mes} and proces_date < {vfecha_proceso} 
    and num_telefonico in (SELECT distinct BAJAS.num_telefonico
    FROM
    (SELECT B.num_telefonico,B.fecha_proceso,B.fecha_ejecucion,B.proces_date
    FROM {vTablaBajInv} B
    WHERE B.proces_date >= {vdia1_mes} AND B.proces_date <= {vfecha_proceso}) BAJAS  
    INNER JOIN 
    (SELECT A.phone_id,A.proces_date,A.counted_days
    FROM db_cs_altas.OTC_T_CHURN_SP2 A
    WHERE A.counted_days <= 90  
    and A.proces_date >= {vdia1_mes} AND A.proces_date <= {vfecha_proceso}) PARQUE  
    ON BAJAS.num_telefonico=PARQUE.phone_id 
    WHERE PARQUE.proces_date >= BAJAS.proces_date
    )
    """.format(vTablaBajInv=vTablaBajInv,vfecha_proceso=vfecha_proceso,vdia1_mes=vdia1_mes)
    print(qry)
    return qry


