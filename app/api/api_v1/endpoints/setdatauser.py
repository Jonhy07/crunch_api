
import io
from fastapi import APIRouter, HTTPException, Request
from fastapi.params import File
from data_connector.db_data.connection import simple_query
from data_connector.db_data.datauser import check_carga, check_potencia, carga_horaria, carga_potencia, check_cuotaamm, check_rro, check_rra
from pydantic import BaseModel, Json
import pandas as pd

router = APIRouter()

class checkcargahoraria(BaseModel):
    name:str
    date:int

class cargahoraria(BaseModel):
    username:str
    fispot:int
    type_fispot:int
    fecha:int
    consumo:str

class cargapotencia(BaseModel):
    username:str
    type_potencia:int
    fecha:int
    value:int


@router.post("/checkcargahoraria")
async def checkcarga(jsonBody: checkcargahoraria):
    return check_carga(jsonBody.name, jsonBody.date)

@router.post("/checkpotencia")
async def checkpotencia(jsonBody: checkcargahoraria):
    return check_potencia(jsonBody.name, jsonBody.date)

@router.post("/checkcuotaamm")
async def checkcuotaamm(jsonBody: checkcargahoraria):
    return check_cuotaamm(jsonBody.name, jsonBody.date)

@router.post("/cargahoraria")
async def cargahoraria(jsonBody: cargahoraria):
    data = io.StringIO(jsonBody.consumo)
    DFS_CONSUMO = pd.read_csv(data)
    return carga_horaria(jsonBody.username, jsonBody.fispot, jsonBody.type_fispot, jsonBody.fecha, DFS_CONSUMO )

@router.post("/cargapotencia")
async def cargapotencia(jsonBody: cargapotencia):
    return carga_potencia(jsonBody.username, jsonBody.fecha, jsonBody.type_potencia, jsonBody.value)

@router.post("/checkrro")
async def checkrro(jsonBody: checkcargahoraria):
    return check_rro(jsonBody.name, jsonBody.date)

@router.post("/checkrra")
async def checkrro(jsonBody: checkcargahoraria):
    return check_rra(jsonBody.name, jsonBody.date)