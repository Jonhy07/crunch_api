from fastapi import APIRouter

from api.api_v1.endpoints import charts, setdatauser, charts_big,genio

api_router = APIRouter()
api_router.include_router(charts.router, prefix="/charts_big", tags=["charts"])
api_router.include_router(charts_big.router, prefix="/charts", tags=["charts"])
api_router.include_router(genio.router, prefix="/genio", tags=["genio"])

api_router.include_router(setdatauser.router, prefix="/setdatauser", tags=["setdatauser"])