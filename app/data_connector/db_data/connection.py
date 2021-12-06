from elasticsearch import Elasticsearch
from datetime import date, datetime
from datetime import timedelta
from core.config import settings
from data_connector.db_translate.endpoint import translate, query_execute

def filtros(filters):
    where=''
    if (len(filters))>0:
        where=' WHERE '
    for filter in filters:
        where+=' '+filter.field+' '+filter.equal+' '+filter.value+ ' AND'
    if( len(where)>0):
        where=where[:-3]
    return where


def simple_query(dataset, x, value, calculate, filters):
    response = {}
    where=filtros(filters)
    query="SELECT "+x+", "+calculate+"("+value+") FROM "+dataset+"  "+where+" group by ("+x+")"
    res=query_execute(query)
    rows = res.fetchall()
    response['series'] = []
    value = []
    for row in rows:
        value.append({'value':row[1], 'name':row[0]})
    response['series'].append({'name':translate(dataset, x), 'data':value}) 
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
        query+="FROM "+dataset+"  "+where+" group by col"
        res=query_execute(query)
        response['xAxis']['name'] = 'Total'
        response['xAxis']['data'] = key
    else:
        if(type_time==1):
            time="'MM/YYYY'"
        elif(type_time==2):
            time="'YYYY'"
        query="SELECT to_char(DATE ("+x+")::date, "+time+") as fecha FROM "+dataset+"  "+where+" group by fecha order by TO_DATE((to_char(DATE ("+x+")::date, "+time+")),"+time+")"
        res=query_execute(query)
        rows = res.fetchall()
        for row in rows:
            key.append(str(row[0]))

        query="Select A.col, B.val "
        query+="from "
        query+="(Select A.fecha, B.col "
        query+="from "
        query+="(SELECT TO_DATE((to_char(DATE ("+x+")::date, "+time+")),"+time+") as fecha "
        query+="FROM "+dataset+" "+where+" group by fecha) As A, "
        query+="(SELECT "+legend+" as col "
        query+="FROM "+dataset+" "+where+" group by col) As B) As A "
        query+="left join "
        query+="(SELECT "+legend+" as col, TO_DATE((to_char(DATE ("+x+")::date, "+time+")),"+time+") as fecha, "+calculate+"("+value+") as val "
        query+="FROM "+dataset+" "+where+" group by col, fecha)As B on A.fecha=B.fecha and A.col=B.col "
        query+="order by (A.col, A.fecha) "
        
        #AQUI
        res=query_execute(query)
        response['xAxis']['name'] = translate(dataset, x)
        response['xAxis']['data'] = key


    rows = res.fetchall()
    list1= ""
    name=""
    if(len (rows)>0):
        name=rows[0][0]

    for row in rows:
        if((name!=row[0])):
            temp1=list(map(float, list1[:-1].split(',') ))
            response['series'].append({"name":name, "data":(temp1)}) 
            name=row[0]
            list1=""
        
        if(row[1]):
            list1+=str(row[1])+','
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
        query+=" from "+dataset+" "
        query+="  "+where
    else:
        if(type_time==1):
            time="'MM/YYYY'"
        elif(type_time==2):
            time="'YYYY'"
        query+="to_char(DATE ("+x+")::date, "+time+") as fecha "
        query+="from "+dataset+" "
        query+="  "+where
        query+="group by (fecha)"
        query+=" order by TO_DATE((to_char(DATE ("+x+")::date, "+time+")),"+time+")"

    res=query_execute(query)
    rows = res.fetchall()
    
    for row in rows:
        n=0
        for atribut in row:
            n=n+1
            if n>c:
                key.append(str(atribut))
            else:
                temp=[]
                temp=value[n]
                temp.append(str(atribut))
                value[n]=temp

    response['xAxis']['name'] = translate(dataset, x)
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
        query="SELECT "+calculate+"(DISTINCT("+field+")) FROM "+dataset+" "+where
    else:
        query="SELECT "+calculate+"("+field+") FROM "+dataset+" "+where
    res=query_execute(query)
    rows = res.fetchall()
    response = {}
    response['head']='card'
    response['data']= rows[0][0]

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
        query+=" from "+dataset+" "
        query+="  "+where
        if groupby!="":
            groupby=groupby[:-1]
            query+="group by ("+groupby+") "
    else:
        for row in columns:
            #c=c+1
            query+=" "+row.field+","
        query=query[:-1]
        query+=" from "+dataset+" "
        query+="  "+where
        query+="order by id"


    res=query_execute(query)
    rows = res.fetchall()
    response = {}
    response['data']=[]
    for row in rows:
        temp=[]
        for atribut in row:
            temp.append(str(atribut))
        response['data'].append(temp)
    return response