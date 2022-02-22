from os import replace
from this import d
from elasticsearch import Elasticsearch
from datetime import date, datetime
from datetime import timedelta
from core.config import settings
from data_connector.db_translate.endpoint import translate, query_execute, query_execute_big_query
import json

def convert_date_to_int(value):
    if value[1].isdigit():
        newvalue=(value.replace("-",""))
    else:
        newvalue=value
    newvalue=(newvalue.replace("'",""))
    return (str(newvalue))

def filtros(filters):
    where=''
    if (len(filters))>0:
        where=' WHERE '
    for filter in filters:
        where+=' '+filter.field+' '+filter.equal+' '+convert_date_to_int(filter.value)+ ' AND'
    if( len(where)>0):
        where=where[:-3]
    return where

def simple_query(dataset, x, value, calculate, filters):
    response = {}
    where=filtros(filters)
    query="SELECT "+x+" as name, "+calculate+"("+value+") as value FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by ("+x+")"
    rows=query_execute_big_query(query)
    response['series'] = []
    value = []
    for row in rows:
        value.append(row)
    response['series'].append({'name': translate(dataset, x), 'data':value}) 
    #response['series'].append({'name': x, 'data':value}) 
    return response

def simple_query1(dataset, x, value, calculate, filters):
    response = {}
    where=filtros(filters)
    query="SELECT "+x+" as name, "+calculate+"("+value+") as value FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by ("+x+") ORDER BY value DESC LIMIT 5"
    rows=query_execute_big_query(query)
    response['series'] = []
    value = []
    for row in rows:
#        value.append({'value':row[1], 'name':row[0]})
        value.append(row)
    response['series'].append({'name': translate(dataset, x), 'data':value}) 
    #response['series'].append({'name': x, 'data':value}) 
    return response

def simple_nested_query(dataset, x, legend,  value, calculate, type_time, filters):
    response = {}
    key = []
    response['xAxis']={}
    response['series']=[]
    res=None
    where=filtros(filters)
    
    if(type_time==3):
        key.append("Total")
        query="SELECT "+legend+" as col, "+calculate+"("+value+") as val "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col LIMIT 15 "
        rows=query_execute_big_query(query)
        response['xAxis']['name'] = 'Total'
        response['xAxis']['data'] = key
    else:
        if(type_time==1):
            time=100
        elif(type_time==2):
            time=10000
        elif(type_time==0):
            time=1
        elif(type_time==4):
            time=7
        query="SELECT CAST (("+x+"/"+str(time)+") AS Integer) as fecha FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by fecha order by fecha"
        rows=query_execute_big_query(query)
        for row in rows:
            key.append(str(row['fecha']))
        query="Select A.col, B.val "
        query+="from "
        query+="(Select A.fecha, B.col "
        query+="from "
        query+="(SELECT CAST (("+x+"/"+str(time)+") AS Integer) as fecha "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by fecha) As A, "
        query+="(SELECT "+legend+" as col "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col) As B) As A "
        query+="left join "
        query+="(SELECT "+legend+" as col, CAST (("+x+"/"+str(time)+") AS Integer) as fecha, "+calculate+"("+value+") as val "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col, fecha)As B on A.fecha=B.fecha and A.col=B.col "
        query+="order by A.col, A.fecha "
        #AQUI
        rows=query_execute_big_query(query)
        response['xAxis']['name'] = translate(dataset, x)
        #response['xAxis']['name'] =  x
        response['xAxis']['data'] = key
    list1= ""
    name=""
    if(len (rows)>0):
        name=rows[0]['col']
    for row in rows:
        if((name!=row['col'])):
            temp1=list(map(float, list1[:-1].split(',') ))
            response['series'].append({"name":name, "data":(temp1)}) 
            name=row['col']
            list1=""
        if(row['val']):
            list1+=str(row['val'])+','
        else:
            list1+='0,'
    if(len(list1)>0):
        response['series'].append({"name":name, "data":list(map(float, list1[:-1].split(',') ))}) 
    return response

def simple_nested_query1(dataset, x, legend,  value, calculate, type_time, filters):
    response = {}
    key = []
    response['xAxis']={}
    response['series']=[]
    res=None
    where=filtros(filters)
    
    if(type_time==3):
        key.append("Total")
        query="SELECT "+legend+" as col, "+calculate+"("+value+") as val "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col "
        rows=query_execute_big_query(query)
        response['xAxis']['name'] = 'Total'
        response['xAxis']['data'] = key
    else:
        if(type_time==1):
            time=100
        elif(type_time==2):
            time=10000
        elif(type_time==0):
            time=1
        elif(type_time==4):
            time=7
        query="SELECT CAST (("+x+"/"+str(time)+") AS Integer) as fecha FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by fecha order by fecha"
        rows=query_execute_big_query(query)
        for row in rows:
            key.append(str(row['fecha']))
        query="Select A.col, B.val "
        query+="from "
        query+="(Select A.fecha, B.col "
        query+="from "
        query+="(SELECT CAST (("+x+"/"+str(time)+") AS Integer) as fecha "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by fecha) As A, "
        query+="(SELECT "+legend+" as col "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col) As B) As A "
        query+="left join "
        query+="(SELECT "+legend+" as col, CAST (("+x+"/"+str(time)+") AS Integer) as fecha, "+calculate+"("+value+") as val "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where+" group by col, fecha)As B on A.fecha=B.fecha and A.col=B.col "
        query+="ORDER BY A.col, A.fecha "
        #query+="ORDER BY B.val DESC LIMIT 15"
        #AQUI
        rows=query_execute_big_query(query)
        response['xAxis']['name'] = translate(dataset, x)
        #response['xAxis']['name'] =  x
        response['xAxis']['data'] = key
    list1= ""
    name=""
    if(len (rows)>0):
        name=rows[0]['col']
    for row in rows:
        if((name!=row['col'])):
            temp1=list(map(float, list1[:-1].split(',') ))
            response['series'].append({"name":name, "data":(temp1)}) 
            name=row['col']
            list1=""
        if(row['val']):
            list1+=str(row['val'])+','
        else:
            list1+='0,'
    if(len(list1)>0):
        response['series'].append({"name":name, "data":list(map(float, list1[:-1].split(',') ))}) 
    return response

def multiple_agg_query(dataset, x, y, type_time, filters):
    response = {}
    key = []
    response['xAxis']={}
    response['series']=[]
    value = {}
    c=0
    query=''
    where=filtros(filters)

    query="Select "
    for row in y:
        c=c+1
        query+=row.calculate+"("+row.value+"), "
        value[c]=[]
    if(type_time==3):
        key.append("Total")
        query=query[:-2]
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
    else:
        if(type_time==1):
            time=100
        elif(type_time==2):
            time=10000
        elif(type_time==0):
            time=1
        elif(type_time==4):
            time=7
        if(time == 7):
            query+="CAST(FORMAT_DATE('%Y%m%d',(DATE_TRUNC(CAST(PARSE_DATE('%Y%m%d', CAST("+ x +" AS STRING)) AS DATE), WEEK))) as INT64) fecha "
        else:
            query+="CAST (("+x+"/"+str(time)+") AS Integer) as fecha "
        query+="FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
        query+="group by fecha "
        query+="order by fecha"
    rows = query_execute_big_query(query)
    
    for row in rows:
        n=0
        for atribut in row:
            n=n+1
            if n>c:
                key.append(str(row[atribut]))
            else:
                temp=[]
                temp=value[n]
                temp.append(str(row[atribut]))
                value[n]=temp

    response['xAxis']['name'] = translate(dataset, x)
    #response['xAxis']['name'] = x
    response['xAxis']['data'] = key

    c=0
    for row in y:
        c=c+1
        response['series'].append({"name":row.name, "data":value[c]})
    return response

def card_query(dataset, field, calculate, type, filters):
    query=None
    where=filtros(filters)
    if type==1:
        query="SELECT "+calculate+"(DISTINCT("+field+")) FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where
    else:
        query="SELECT "+calculate+"("+field+") FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "+where
    rows=query_execute_big_query(query)
    response = {}
    response['head']='card'
    if(rows[0]['f0_'] == None):
        response['data']=0
    else:
        response['data']=rows[0]['f0_']
    response['data']= "{:,.2f}".format(response['data'])
    return response

def tab_query(dataset, columns, type, filters):
    where=filtros(filters)
    query="Select"
    if type==1:
        groupby=""
        for row in columns:
            if (row.calculate):
                query+= " "+row.calculate+"("+row.field+"),"
            else:
                query+= " "+row.field+","
                groupby+= " "+row.field+","
        query=query[:-1]
        query+=" FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
        if groupby!="":
            groupby=groupby[:-1]
            query+="group by "+groupby+" "
    else:
        for row in columns:
            #c=c+1
            query+=" "+row.field+","
        query=query[:-1]
        query+=" FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
        #query+="order by id"

def tab_front_query(dataset, columns, type, filters, length, start ):
    where=filtros(filters)
    query="Select"
    if type==1:
        groupby=""
        for row in columns:
            if (row.calculate):
                query+= " "+row.calculate+"("+row.field+"),"
            else:
                query+= " "+row.field+","
                groupby+= " "+row.field+","
        query=query[:-1]
        query+=" FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
        if groupby!="":
            groupby=groupby[:-1]
            query+="group by "+groupby+" "
        query+=" LIMIT "+str(length)
        query+=" OFFSET  "+str(start)
    else:
        for row in columns:
            query+=" "+row.field+","
        query=query[:-1]
        query+=" FROM `"+settings.BIG_QUERY_DB_DATA_NAME+"."+dataset+"` "
        query+="  "+where
        query+=" LIMIT "+str(length)
        query+=" OFFSET  "+str(start)
    rows = query_execute_big_query(query)
    response = {}
    response['data']=[]
    for row in rows:
        temp=[]
        for atribut in row:
            temp.append(str(row[atribut]))
        response['data'].append(temp)
    return response

def tab_front_query_sp(dataset, type, filters, length, start):
    tienda=str(getattr(filters[0],'store'))
    max=int(getattr(filters[0],'max'))
    min=int(getattr(filters[0],'min'))
    marketplace=str(getattr(filters[0],'Marketplace'))
    plataforma=str(getattr(filters[0],'Plataforma'))
    query="CALL Stage.{}({},{},{},'{}','{}')".format(dataset,tienda,int(max),int(min),str(marketplace),str(plataforma))
    print('*****************************')
    print(query)
    print('*****************************')
    rows = query_execute_big_query(query)
    response = {}
    response['data']=[]
    for row in rows:
        temp=[]
        for atribut in row:
            temp.append(str(row[atribut]))
        response['data'].append(temp)
    return response

def prueba_big():
    query = (
        'SELECT DnStoreId, DxIdDataFrame, DxMarketplace, DxAsin, DxSellerSku, DxProductName, DxStoreName, DnFechaCarga '
        'FROM `'+settings.BIG_QUERY_DB_DATA_NAME+'.AuxDimInventario` '
        'LIMIT 100')
    result=query_execute_big_query(query)
    #json_obj = json.dumps(str(result))
    return result