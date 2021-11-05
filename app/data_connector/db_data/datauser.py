from logging import error
from data_connector.db_translate.endpoint import query_execute
from core.config import settings
from sqlalchemy import create_engine
import traceback


def insert_liquidacion_valorizacion(type_fispot, id_user, fecha):
    query=""
    if type_fispot==1:
        query="SELECT SUM(((P.valor+(FS.value*P.valor/100))*(EC.kwhd/1000)))as ValorizacionEnergia FROM public.energia_consumida as EC inner join public.precio_spot_kw_mes as P on EC.fecha=P.fecha and EC.hora=P.hora inner join public.fispot as FS on EC.id_user=FS.id_user where P.fecha>"+str(fecha)+" and P.fecha<"+str(fecha+32)+" and FS.id_user="+str(id_user)+" and FS.fecha="+str(fecha)
    else:
        query="SELECT SUM(((P.valor+(FS.value))*(EC.kwhd/1000)))as ValorizacionEnergia FROM public.energia_consumida as EC inner join public.precio_spot_kw_mes as P on EC.fecha=P.fecha and EC.hora=P.hora inner join public.fispot as FS on EC.id_user=FS.id_user where P.fecha>"+str(fecha)+" and P.fecha<"+str(fecha+32)+" and FS.id_user="+str(id_user)+" and FS.fecha="+str(fecha)
    res=query_execute(query)
    valor=(res.fetchall()[0][0])
    query2="INSERT INTO public.liquidacion( fecha, id_user, valorizacion_energia) VALUES ("+str(fecha)+", "+str(id_user)+","+str(valor)+")"
    res=query_execute(query2)


def insert_liquidacion_potencia(type_potencia, id_user, fecha):
    query=""
    if type_potencia==1:
        anio=int(fecha/10000)
        anio=anio*10000
        query="SELECT (DF.value*PKM.value) as Valorizacion_Potencia FROM public.demanda_firme as DF, public.potencia as PKM where DF.fecha="+str(anio)+" and PKM.fecha="+str(fecha)+" and PKM.id_user="+str(id_user)
    else:
        query="SELECT (MAX (EC.kwhd)* PKM.value )as Valorizacion_Potencia FROM public.energia_consumida as EC inner join public.potencia as PKM on EC.id_user=PKM.id_user where EC.fecha>"+str(fecha)+" and EC.fecha<"+str(fecha+32)+" and PKM.fecha="+str(fecha)+" and EC.id_user="+str(id_user)+" group by (PKM.value)"
    res=query_execute(query)
    valor=(res.fetchall()[0][0])
    query2="UPDATE public.liquidacion SET potencia="+str(valor)+" WHERE id_user="+str(id_user)+" and fecha="+str(fecha)
    #try:
    res=query_execute(query2)
    #except Exception:
    #    traceback.print_exc()

def insert_cuota_amm(id_user, name, fecha, id_type_user):
    user=" "+name
    query=""
    if(id_type_user<3):
        query="SELECT sum (potencia) FROM public.cuota_amm_a where participante='"+user+"' and  fecha="+str(fecha)+" and (nombre='VALORIZACIÓN DE LA PRODUCCIÓN (US$)' or nombre='VALORIZACIÓN DE LA COMERCIALIZACIÓN (US$)') group by (participante, fecha)"
    else:
        query="SELECT sum(valor) FROM public.cuota_amm_b WHERE fecha>"+str(fecha)+" and fecha<"+str(fecha+32)+" and nombre='"+str(name)+"' group by (nombre);"
    res=query_execute(query)
    participacion_usuario=(res.fetchall()[0][0])
    query="SELECT value FROM public.total_amm where fecha="+str(fecha)
    res=query_execute(query)
    participacion_total=(res.fetchall()[0][0])

    anio=int(fecha/10000)
    anio=anio*10000
    query="SELECT value FROM public.presupuesto_anual_amm where fecha="+str(anio)
    res=query_execute(query)
    presupuesto_anual=(res.fetchall()[0][0])
    factor_participacion=float((float(participacion_usuario))/(float(participacion_total)))
    presupuesto_mensual=float((float(presupuesto_anual))/12)
    cuota_mensual=float(factor_participacion*presupuesto_mensual)
    query2="INSERT INTO public.cuota_amm(id_user, fecha, suma, factor_participacion, value) VALUES ("+str(id_user)+", "+str(fecha)+", "+str(participacion_usuario)+", "+str(factor_participacion)+", "+str(cuota_mensual)+")"
    res=query_execute(query2)

    query3="UPDATE public.liquidacion SET amm="+str(cuota_mensual)+" WHERE id_user="+str(id_user)+ " AND fecha="+str(fecha)
    res=query_execute(query3)


def insert_liquidacion_rro(id_liquidacion, fecha, id_user):
    query="select sum(A.rro) as rro_total from (select ((sum(mw.potencia*prr.precio)/dh.demanda_sni)*(ec.kwhd/1000)) as rro FROM public.mw_rro_asignados as mw  inner join precio_reserva_rodante as prr on mw.fecha=prr.fecha and mw.hora=prr.hora and mw.nombre=prr.nombre inner join demanda_horaria as dh on mw.fecha=dh.fecha and mw.hora=dh.hora inner join energia_consumida as ec on mw.fecha=ec.fecha and mw.hora=ec.hora where dh.tipo_carga=1 and ec.id_user="+str(id_user)+" and mw.fecha> "+str(fecha)+" and mw.fecha<"+str(fecha+32)+" and prr.fecha> "+str(fecha)+" and prr.fecha<"+str(fecha+32)+" and dh.fecha> "+str(fecha)+" and dh.fecha<"+str(fecha+32)+" and ec.fecha> "+str(fecha)+" and ec.fecha<"+str(fecha+32)+" group by (dh.demanda_sni,ec.kwhd)) as A"
    res=query_execute(query)
    rows = res.fetchall()
    rro=rows[0][0]
    query2="UPDATE public.liquidacion SET rro="+str(rro)+" WHERE id="+str(id_liquidacion)
    res=query_execute(query2)


def insert_liquidacion_rra(id_liquidacion, fecha, id_user):
    query="select sum(rra_parcial.rra) as total from (SELECT  ((((sum (rra.total))/24) / dh.demanda_sni)*(ec.kwhd/1000)) as rra FROM public.reserva_rapida_rra as rra inner join demanda_horaria as dh on rra.fecha=dh.fecha inner join energia_consumida as ec on dh.fecha=ec.fecha and dh.hora=ec.hora where dh.tipo_carga=1 and dh.fecha>"+str(fecha)+" and dh.fecha < "+str(fecha+32)+" and rra.fecha>"+str(fecha)+" and rra.fecha < "+str(fecha+32)+" and  ec.fecha>"+str(fecha)+" and ec.fecha < "+str(fecha+32)+" and ec.id_user="+str(id_user)+" group by (rra.fecha, dh.hora, dh.demanda_sni,ec.kwhd)) as rra_parcial"
    res=query_execute(query)
    rows = res.fetchall()
    rra=rows[0][0]
    query2="UPDATE public.liquidacion SET rra="+str(rra)+" WHERE id="+str(id_liquidacion)
    res=query_execute(query2)


def check_carga(name, date):    
    response = {}
    
    query= "SELECT id FROM public.users WHERE name='"+name+"'"
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        response = {'status':0}
        #No existe usuario
        return response
    id_user=rows[0][0]
    query="SELECT id FROM public.fispot WHERE id_user="+str(id_user)+" AND fecha="+str(date)
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        query="SELECT (index) FROM public.precio_spot_kw_mes where fecha>"+str(date)+" and fecha<"+str(date+32)
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':3}
        else:
            response = {'status':2}
        #No hay registro se puede ingresar
        return response
    response = {'status':1}
    #si hay registros no se puede ingresar
    return response


def check_potencia(name, date):    
    response = {}
    query= "SELECT id FROM public.users WHERE name='"+name+"'"
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        response = {'status':0}
        return response
    id_user=rows[0][0]
    
    
    query="SELECT id FROM public.liquidacion WHERE id_user="+str(id_user)+" AND fecha="+str(date)
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        response = {'status':3}
        #No se a calculado la valorizacion
        return response
    
    query="SELECT id FROM public.potencia WHERE id_user="+str(id_user)+" AND fecha="+str(date)
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        response = {'status':2}
        #No hay registro se puede ingresar
        return response
    response = {'status':1}
    #si hay registros no se puede ingresar
    return response



def check_cuotaamm(name, date):
    try:
        response = {}
        query= "SELECT id, id_type_user FROM public.users WHERE name='"+name+"'"
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':0}
            #No existe usuario
            return response
        id_user=rows[0][0]
        id_type_user=rows[0][1]
        query="SELECT id FROM public.cuota_amm WHERE id_user="+str(id_user)+" AND fecha="+str(date)
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            anio=int(date/10000)
            anio=anio*10000
            query="SELECT id FROM public.presupuesto_anual_amm WHERE fecha="+str(anio)
            res=query_execute(query)
            rows = res.fetchall()
            if(len(rows)>0):
                insert_cuota_amm(id_user, name, date, id_type_user)
                response = {'status':2}
                #No hay registro se puede ingresar
            else:
                response = {'status':3}
                #No hay presupuesto anual
            return response
        response = {'status':1}
        #si hay registros no se puede ingresar
        return response
    except:
        return {'status':4}




def carga_horaria(username, fispot, type_fispot, fecha, DFS_CONSUMO ):
    response = {}
    query= "SELECT id FROM public.users WHERE name='"+username+"'"
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        #no existe el usuario
        response = {'status':0}
        return response
    try:
        id_user=rows[0][0]
        DFS_CONSUMO['id_user']= int(id_user)
        SQLALCHEMY_DATABASE_URL_DATA = "postgresql://"+settings.ENDPOINT_DB_DATA_USER+":"+settings.ENDPOINT_DB_DATA_PASS+"@"+settings.ENDPOINT_DB_DATA_HOST+":"+settings.ENDPOINT_DB_DATA_PORT+"/"+settings.ENDPOINT_DB_DATA_NAME
        engine_data = create_engine(SQLALCHEMY_DATABASE_URL_DATA)
        DFS_CONSUMO.to_sql("energia_consumida", con=engine_data, if_exists="append")
        query="INSERT INTO public.fispot( id_user, fecha, id_fispottype, value) VALUES ("+str(id_user)+", "+str(fecha)+", "+str(type_fispot)+", "+str(fispot)+")"
        res=query_execute(query)
        insert_liquidacion_valorizacion(type_fispot, id_user, fecha)
        #Retorno correcto
        response = {'status':2}
        return response
    except:
        #Hubo un error
        response = {'status':1}
        return response

def carga_potencia(username, fecha, type_potencia, value):
    response = {}
    query= "SELECT id FROM public.users WHERE name='"+username+"'"
    res=query_execute(query)
    rows = res.fetchall()
    if(len(rows)<1):
        #no existe el usuario
        response = {'status':0}
        return response
    try:
        id_user=rows[0][0]
        query="INSERT INTO public.potencia( id_user, fecha, id_potenciatype, value) VALUES ("+str(id_user)+", "+str(fecha)+", "+str(type_potencia)+", "+str(value)+")"
        res=query_execute(query)
        insert_liquidacion_potencia(type_potencia, id_user, fecha)
        #Retorno correcto
        response = {'status':2}
        return response
    except:
        #Hubo un error
        response = {'status':1}
        return response




def check_rro(name, date):
    try:
        response = {}
        query= "SELECT id FROM public.users WHERE name='"+name+"'"
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':0}
            #No existe el usuario
            return response
        id_user=rows[0][0]
        
        
        query="SELECT id, rro FROM public.liquidacion WHERE id_user="+str(id_user)+" AND fecha="+str(date)
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':3}
            #No se a calculado la valorizacion
            return response
        
        id_liquidacion=rows[0][0]
        rro=rows[0][1]
        if(rro):
            #existe rro
            response = {'status':1}
        else:
            #Se puede hacer el update
            insert_liquidacion_rro(id_liquidacion, date, id_user)
            response = {'status':2}
    except:
        return {'status':4}
    return response



def check_rra(name, date):
    try:
        response = {}
        query= "SELECT id FROM public.users WHERE name='"+name+"'"
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':0}
            #No existe el usuario
            return response
        id_user=rows[0][0]
        
        
        query="SELECT id, rra FROM public.liquidacion WHERE id_user="+str(id_user)+" AND fecha="+str(date)
        res=query_execute(query)
        rows = res.fetchall()
        if(len(rows)<1):
            response = {'status':3}
            #No se a calculado la valorizacion
            return response
        
        id_liquidacion=rows[0][0]
        rra=rows[0][1]
        if(rra):
            #existe rro
            response = {'status':1}
        else:
            #Se puede hacer el update
            insert_liquidacion_rra(id_liquidacion, date, id_user)
            response = {'status':2}
    except:
        return {'status':4}
    return response