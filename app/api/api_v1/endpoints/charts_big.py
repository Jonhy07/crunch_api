from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException, Request
from data_connector.db_data.connection_big import simple_query, simple_nested_query, multiple_agg_query, card_query, tab_query, tab_front_query, prueba_big,simple_query1,simple_nested_query1,tab_front_query_sp
from pydantic import BaseModel, Json
from typing import Dict, Optional

from fastapi import FastAPI
from typing import Dict, AnyStr, Union

router = APIRouter()

class filter(BaseModel):
    field:str
    equal:str
    value:Optional[str]

class filter_sp(BaseModel):
    store:str
    min:str
    max:str
    Marketplace:str
    Plataforma:str

class column(BaseModel):
    field:str
    calculate:Optional[str]

class simpleYRow(BaseModel):
    value:str
    calculate:str

class YRow(BaseModel):
    name:str
    value:str
    calculate:str

class JsonBodyPie(BaseModel):
    dataset:str
    x:str
    y:simpleYRow
    filters:List[filter]

class JsonBody(BaseModel):
    dataset:str
    type:int
    type_time:int
    x:str
    y:List[YRow]
    filters:List[filter]

class JsonTab(BaseModel):
    type:Optional [str]
    columns:List[column]
    dataset:str
    filters:List[filter]

class JsonTab_Front(BaseModel):
    dataset:str
    type:Optional [int]
    columns:List[column]
    filters:List[filter]
    length:int
    start:int

class JsonTab_FrontSp(BaseModel):
    dataset:str
    type:Optional [int]
    filters:List[filter_sp]
    length:int
    start:int

@router.post("/pie")
async def im_pie(jsonBody : JsonBodyPie):
    return simple_query1(jsonBody.dataset, jsonBody.x, jsonBody.y.value, jsonBody.y.calculate, jsonBody.filters)

@router.post("/line_bar")
async def im_line_bar(jsonBody : JsonBody):
    if jsonBody.type==1:
        return simple_nested_query1(jsonBody.dataset, jsonBody.x, jsonBody.y[0].name, jsonBody.y[0].value, jsonBody.y[0].calculate, jsonBody.type_time, jsonBody.filters)
    else:
        return multiple_agg_query(jsonBody.dataset, jsonBody.x,  jsonBody.y, jsonBody.type_time, jsonBody.filters)

@router.post("/card")
def im_card(jsonBody : JsonTab):
    return card_query(jsonBody.dataset, jsonBody.columns[0].field, jsonBody.columns[0].calculate, jsonBody.type, jsonBody.filters)

@router.post("/tab")
async def im_tab(jsonBody : JsonTab):
    return tab_query(jsonBody.dataset, jsonBody.columns, jsonBody.type, jsonBody.filters)

@router.post("/tab_front")
def im_tab_front(jsonBody : JsonTab_Front):
    return tab_front_query(jsonBody.dataset, jsonBody.columns, jsonBody.type, jsonBody.filters,jsonBody.length, jsonBody.start )

@router.post("/tab_front_sp")
def im_tab_front(jsonBody : JsonTab_FrontSp):
    return tab_front_query_sp(jsonBody.dataset, jsonBody.type, jsonBody.filters,jsonBody.length, jsonBody.start )

@router.get("/prueba")
async def prueba():
    return prueba_big()

JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = Union[JSONArray, JSONObject]

@router.post("/tabla2")
async def im_tab2(arbitrary_json: JSONStructure = None):
    return {"received_data": arbitrary_json}