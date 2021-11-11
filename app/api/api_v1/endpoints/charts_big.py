from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException, Request
from data_connector.db_data.connection_big import simple_query, simple_nested_query, multiple_agg_query, card_query, tab_query, tab_front_query, prueba_big
from pydantic import BaseModel, Json
from typing import Dict, Optional

from fastapi import FastAPI
from typing import Dict, AnyStr, Union

router = APIRouter()

class filter(BaseModel):
    field:str
    equal:str
    value:Optional[str]

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
    dataset:str
    type:Optional [int]
    columns:List[column]
    filters:List[filter]


class JsonTab_Front(BaseModel):
    dataset:str
    type:Optional [int]
    columns:List[column]
    filters:List[filter]
    length:int
    start:int

@router.post("/pie")
async def im_pie(jsonBody : JsonBodyPie):
    return simple_query(jsonBody.dataset, jsonBody.x, jsonBody.y.value, jsonBody.y.calculate, jsonBody.filters)

@router.post("/line_bar")
async def im_line_bar(jsonBody : JsonBody):
    if jsonBody.type==1:
        return simple_nested_query(jsonBody.dataset, jsonBody.x, jsonBody.y[0].name, jsonBody.y[0].value, jsonBody.y[0].calculate, jsonBody.type_time, jsonBody.filters)
    else:
        return multiple_agg_query(jsonBody.dataset, jsonBody.x,  jsonBody.y, jsonBody.type_time, jsonBody.filters)


@router.post("/card")
async def im_card(jsonBody : JsonTab):
    return card_query(jsonBody.dataset, jsonBody.columns[0].field, jsonBody.columns[0].calculate, jsonBody.type, jsonBody.filters)


@router.post("/tab")
async def im_tab(jsonBody : JsonTab):
    return tab_query(jsonBody.dataset, jsonBody.columns, jsonBody.type, jsonBody.filters)



@router.post("/tab_front")
async def im_tab_front(jsonBody : JsonTab_Front):
    return tab_front_query(jsonBody.dataset, jsonBody.columns, jsonBody.type, jsonBody.filters,jsonBody.length, jsonBody.start )



@router.get("/prueba")
async def prueba():
    return prueba_big()



JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = Union[JSONArray, JSONObject]


@router.post("/tabla2")
async def im_tab2(arbitrary_json: JSONStructure = None):
    print('..Prueba1....')
    print(arbitrary_json)
    return {"received_data": arbitrary_json}