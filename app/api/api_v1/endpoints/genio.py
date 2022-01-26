from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException, Request
from data_connector.genio_functions.connection import get_notifications,confirmacion_lectura,confirmacion_utilidad,cambio_estado,get_notifications_details
from pydantic import BaseModel, Json
from typing import Dict, Optional

from fastapi import FastAPI
from typing import Dict, AnyStr, Union

router = APIRouter()

class parametros(BaseModel):
    store:str
    fecha:str
    type:Optional[str]


class confirmacion(BaseModel):
    store:str
    fecha:str
    tipo:str
    estado:str

class estado(BaseModel):
    store:str
    tipo:str
    estado:str

@router.post("/notificaciones")
async def notificaciones(jsonBody : parametros):
    return get_notifications(jsonBody.store,jsonBody.fecha)

@router.post("/notificaciones_detalle")
async def notificaciones_detalle(jsonBody : parametros):
    return get_notifications_details(jsonBody.store,jsonBody.fecha)

@router.post("/confirmacion_lectura")
async def notificaciones(jsonBody : confirmacion):
    return confirmacion_lectura(jsonBody.store,jsonBody.fecha,jsonBody.tipo,jsonBody.estado)

@router.post("/confirmacion_utilidad")
async def notificaciones(jsonBody : confirmacion):
    return confirmacion_utilidad(jsonBody.store,jsonBody.fecha,jsonBody.tipo,jsonBody.estado)

@router.post("/cambio_estado")
async def notificaciones(jsonBody : estado):
    return cambio_estado(jsonBody.store,jsonBody.tipo,jsonBody.estado)