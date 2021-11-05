from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from core.config import settings
from sqlalchemy import text

from google.cloud import bigquery
import json

#TRADUCIR
SQLALCHEMY_DATABASE_URL = "postgresql://"+settings.ENDPOINT_DB_USER+":"+settings.ENDPOINT_DB_PASS+"@"+settings.ENDPOINT_DB_HOST+":"+settings.ENDPOINT_DB_PORT+"/"+settings.ENDPOINT_DB_NAME
engine = create_engine(SQLALCHEMY_DATABASE_URL)


def translate(dataset, db_column):
	with engine.connect() as connection:
		id = 0
		name_bc = ''
		dataset_object = connection.execute(text("select id from endpoint_endpoint where name_db = '"+dataset+"'"))
		for row in dataset_object:
			id = row['id']
		result = connection.execute(text("select name_bc from endpoint_detail where name_db = '"+db_column+"' and endpoint_id = "+str(id)))
		for row in result:
			name_bc = row['name_bc']
		return name_bc


#QUERY POSTGRES
SQLALCHEMY_DATABASE_URL_DATA = "postgresql://"+settings.ENDPOINT_DB_DATA_USER+":"+settings.ENDPOINT_DB_DATA_PASS+"@"+settings.ENDPOINT_DB_DATA_HOST+":"+settings.ENDPOINT_DB_DATA_PORT+"/"+settings.ENDPOINT_DB_DATA_NAME
engine_data = create_engine(SQLALCHEMY_DATABASE_URL_DATA)


def query_execute(query):
	with engine_data.connect() as connection_data:
		dataset_object = connection_data.execute(text(query))
		return dataset_object


#QUERY BIG_QUERY
def query_execute_big_query(query):
    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records