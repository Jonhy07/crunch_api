from os import replace
from elasticsearch import Elasticsearch
from datetime import date, datetime
from datetime import timedelta
from core.config import settings
from data_connector.db_translate.endpoint import translate, query_execute, query_execute_big_query,query_execute_big_query_df
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def get_notifications(store,fecha):
	query="""
	SELECT por_detalle.*,A.mensaje FROM (SELECT store_id,funcion,fecha,count(*) AS resultados
		FROM genio.notificaciones_detalle WHERE store_id={} AND fecha='{}'
		GROUP BY store_id,funcion,fecha) por_detalle
	LEFT JOIN genio.notificaciones A on por_detalle.store_id = A.store_id and por_detalle.fecha=A.fecha and por_detalle.funcion = A.funcion""".format(store,fecha)
	rows=query_execute_big_query(query)
	return rows

def get_notifications_details(store,fecha):
	notificaciones_generales=get_notifications(store,fecha)
	query="SELECT * FROM genio.notificaciones_detalle WHERE store_id = {} AND fecha = '{}'".format(store,fecha)
	allN=query_execute_big_query_df(query)
	contador =-1
	for i in notificaciones_generales:
		contador+=1
		df_t=allN.loc[(allN['funcion']==i['funcion']),:][:2]
		df_t.reset_index(drop=True,inplace=True)
		notificaciones_generales[contador]['items']=df_t.to_dict('records')
	return notificaciones_generales


def confirmacion_lectura(store,fecha,tipo,estado):
	query="UPDATE genio.notificaciones SET util = {} WHERE store_id = {} AND funcion = '{}' AND fecha = '{}'".format(estado,store,tipo,fecha)
	try:
		rows=query_execute_big_query(query)
		return 'success'
	except:
		return 'error'

def confirmacion_utilidad(store,fecha,tipo,estado):
	query="UPDATE genio.notificaciones SET lectura = {} WHERE store_id = {} AND funcion = '{}' AND fecha = '{}'".format(estado,store,tipo,fecha)
	try:
		rows=query_execute_big_query(query)
		return 'success'
	except:
		return 'error'

def cambio_estado(store,tipo,estado):
	query="UPDATE genio.notificaciones_permisos SET habilitado = {} WHERE store_id = {} AND tipo = '{}'".format(estado,store,tipo)
	try:
		rows=query_execute_big_query(query)
		return 'success'
	except:
		return 'error'