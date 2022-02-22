from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException, Request
from data_connector.genio_functions.connection import get_notifications,confirmacion_lectura,confirmacion_utilidad,cambio_estado,get_notifications_details,get_last_date,get_notifications_complete,semana_desde,mes_desde,historico_mensual,agrupacion_por_tienda
from pydantic import BaseModel, Json
from typing import Dict, Optional

from fastapi import FastAPI
from typing import Dict, AnyStr, Union

router = APIRouter()

class parametros(BaseModel):
    store:str
    fecha:str
    funcion:Optional[str]

class confirmacion(BaseModel):
    store:str
    fecha:str
    tipo:str
    estado:str

class estado(BaseModel):
    store:str
    tipo:str
    estado:str

class store(BaseModel):
    store:str

class meses(BaseModel):
    store:str
    mes:int
    anio:int

class store_dos_fechas(BaseModel):
    store:str
    fecha_min:str
    fecha_max:str

@router.post("/notificaciones")
async def notificaciones(jsonBody : parametros):
    return get_notifications(jsonBody.store,jsonBody.fecha)

@router.post("/notificaciones_detalle")
async def notificaciones_detalle(jsonBody : parametros):
    return get_notifications_details(jsonBody.store,jsonBody.fecha,jsonBody.funcion)

@router.post("/notificaciones_historico")
async def notificaciones_historico(jsonBody : store):
    return get_notifications_complete(jsonBody.store)

@router.post("/semana_desde")
async def notificaciones_semanales(jsonBody : parametros):
    return semana_desde(jsonBody.store,jsonBody.fecha)

@router.post("/historico_mensual")
async def notificaciones_mes(jsonBody : meses):
    return historico_mensual(jsonBody.store,jsonBody.anio,jsonBody.mes)

@router.post("/mes_desde")
async def notificaciones_mensuales(jsonBody : meses):
    return mes_desde(jsonBody.store,jsonBody.mes,jsonBody.anio)

@router.post("/confirmacion_lectura")
async def notificaciones(jsonBody : confirmacion):
    return confirmacion_lectura(jsonBody.store,jsonBody.fecha,jsonBody.tipo,jsonBody.estado)

@router.post("/confirmacion_utilidad")
async def notificaciones(jsonBody : confirmacion):
    return confirmacion_utilidad(jsonBody.store,jsonBody.fecha,jsonBody.tipo,jsonBody.estado)

@router.post("/cambio_estado")
async def notificaciones(jsonBody : estado):
    return cambio_estado(jsonBody.store,jsonBody.tipo,jsonBody.estado)

@router.post("/ultima_fecha")
async def last_date(jsonBody : store):
    return get_last_date(jsonBody.store)

@router.post("/agrupacion_por_tienda")
async def agrupacion_tienda(jsonBody : store_dos_fechas):
    return agrupacion_por_tienda(jsonBody.store,jsonBody.fecha_min,jsonBody.fecha_max)