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

def name_to_month(mes):
	switcher = {
		1: "Enero",
		2: "Febrero",
		3: "Marzo",
		4: "Abril",
		5: "Mayo",
		6: "Junio",
		7: "Julio",
		8: "Agosto",
		9: "Septiembre",
		10: "Octubre",
		11: "Noviembre",
		12: "Diciembre"
	}
	return switcher[mes]

def get_notifications(store,fecha):
	query="""
	SELECT por_detalle.*,A.mensaje,A.util FROM (SELECT store_id,funcion,fecha,count(*) AS resultados
		FROM genio.notificaciones_detalle WHERE store_id={} AND fecha='{}'
		GROUP BY store_id,funcion,fecha) por_detalle
	LEFT JOIN genio.notificaciones A on por_detalle.store_id = A.store_id and por_detalle.fecha=A.fecha and por_detalle.funcion = A.funcion""".format(store,fecha)
	rows=query_execute_big_query(query)
	return rows

def get_notifications_details(store,fecha,funcion):
	if funcion == None:
		notificaciones_generales=get_notifications(store,fecha)
		query="SELECT * FROM genio.notificaciones_detalle WHERE store_id = {} AND fecha = '{}'".format(store,fecha)
		allN=query_execute_big_query_df(query)
		contador =-1
		for i in notificaciones_generales:
			contador+=1
			df_t=allN.loc[(allN['funcion']==i['funcion']),:][:10]
			df_t.reset_index(drop=True,inplace=True)
			notificaciones_generales[contador]['items']=df_t.to_dict('records')
		return notificaciones_generales
	else:
		query="SELECT * FROM genio.notificaciones WHERE store_id = {} AND fecha = '{}' AND funcion='{}'".format(store,fecha,funcion)
		general=query_execute_big_query(query)
		query="SELECT * FROM genio.notificaciones_detalle WHERE store_id = {} AND fecha = '{}' AND funcion='{}'".format(store,fecha,funcion)
		specific=query_execute_big_query(query)
		general=general[0]
		general['items']=specific
		return general

def get_notifications_complete(store):
	query="SELECT fecha FROM genio.notificaciones WHERE store_id = {} GROUP BY fecha ORDER BY fecha DESC LIMIT 7".format(store)
	week=query_execute_big_query(query)
	query="SELECT anio,mes FROM (SELECT store_id,EXTRACT(YEAR from fecha) as anio,EXTRACT(MONTH from fecha) as mes FROM genio.notificaciones WHERE store_id = {}) WHERE store_id = {} GROUP BY anio,mes ORDER BY anio DESC,mes DESC LIMIT 7".format(store,store)
	month=query_execute_big_query(query)
	semana=[]
	for i in week:
		semana.append(i['fecha'])

	for i in range(len(month)):
		month[i]['mes']=name_to_month(month[i]['mes'])
	return {'semana':semana,'mes':month}

def historico_mensual(store,anio,mes):
	query="SELECT fecha FROM genio.notificaciones WHERE store_id = {} and EXTRACT(MONTH FROM fecha)={} and EXTRACT(YEAR FROM fecha)={} GROUP BY fecha ORDER BY fecha DESC".format(store,mes,anio)
	dias_r=query_execute_big_query(query)
	dias=[]
	for i in dias_r:
		dias.append(i['fecha'])
	return {'dias_mes':dias}

def semana_desde(store,fecha):
	query="SELECT fecha FROM genio.notificaciones WHERE store_id = {} AND fecha < '{}' GROUP BY fecha ORDER BY fecha DESC LIMIT 7".format(store,fecha)
	week=query_execute_big_query(query)
	semana=[]
	for i in week:
		semana.append(i['fecha'])
	return {'semana':semana}

def mes_desde(store,mes,anio):
	fecha = (anio*100+mes)*100 + 1
	query="SELECT anio,mes FROM (SELECT store_id,EXTRACT(YEAR from fecha) as anio,EXTRACT(MONTH from fecha) as mes FROM genio.notificaciones WHERE store_id = {} and fecha < PARSE_DATE('%Y%m%d', CAST({} AS STRING)) ) WHERE store_id = {} GROUP BY anio,mes ORDER BY anio,mes DESC LIMIT 7".format(store,fecha,store)
	month=query_execute_big_query(query)
	return {'mes':month}

def get_last_date(store):
	query="select fecha from genio.notificaciones where store_id = {} order by fecha desc limit 1".format(store)
	general=query_execute_big_query(query)
	return general

def confirmacion_lectura(store,fecha,tipo,estado):
	query="UPDATE genio.notificaciones SET lectura = {} WHERE store_id = {} AND funcion = '{}' AND fecha = '{}'".format(estado,store,tipo,fecha)
	try:
		print(query)
		query_execute_big_query(query)
		return 'success'
	except:
		print('error')
		return 'error'

def confirmacion_utilidad(store,fecha,tipo,estado):
	if estado == 'True':
		state=True
	else:
		state=False
	query="UPDATE genio.notificaciones SET util = {} WHERE store_id = {} AND funcion = '{}' AND fecha = '{}'".format(state,store,tipo,fecha)
	try:
		print(query)
		query_execute_big_query(query)
		return 'success'
	except:
		print('error')
		return 'error'

def cambio_estado(store,tipo,estado):
	query="UPDATE genio.notificaciones_permisos SET habilitado = {} WHERE store_id = {} AND tipo = '{}'".format(estado,store,tipo)
	try:
		rows=query_execute_big_query(query)
		return 'success'
	except:
		return 'error'

def agrupacion_por_tienda(store,fecha_min,fecha_max):
	query1="SELECT DxIdDataFrame FROM Stage.AuxDimVentas WHERE DnStoreId = {} AND DnPurchaseDate >= {} AND DnPurchaseDate <= {} GROUP BY DxIdDataFrame".format(store,fecha_min,fecha_max)
	DxIdDataframe=query_execute_big_query(query1)
	plataformas=[]
	for i in DxIdDataframe:
		plataformas.append(i['DxIdDataFrame'])

	query2="SELECT DxMarketplace FROM Stage.AuxDimVentas WHERE DnStoreId = {} AND DnPurchaseDate >= {} AND DnPurchaseDate <= {} GROUP BY DxMarketplace".format(store,fecha_min,fecha_max)
	DxIdDataframe=query_execute_big_query(query2)
	marketplace=[]
	for i in DxIdDataframe:
		marketplace.append(i['DxMarketplace'])
	print('--')
	print(query1)
	print(query2)
	print('--')
	return {'plataformas':plataformas,'marketplace':marketplace}