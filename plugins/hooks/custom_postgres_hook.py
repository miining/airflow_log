from typing import Any
from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id,**kwargs):
        self.postgres_conn_id = postgres_conn_id
    
    
    def get_conn(self):
        #get_connection은 airflow ui connection에서 제공해주는 정보를 가져옴 
        airflow_conn  = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        #get_conn메소드는 db와 연결된 객체를 반환
        self.postgres_conn_id = psycopg2.connect(host = self.host,user = self.user,password=self.password,dbname = self.dbname,port = self.port)
        return self.postgres_conn_id
    
    
    def bulk_load(self,table_name,file_name,delimiter:str,is_header:bool,is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:'+file_name)
        self.log.info('테이블:'+table_name)
        #self 변수들을 사용하기 위해 함수만 사용,return 값을 당장 필요하지는 않음
        self.get_conn()

        #header-true면 0, 그렇지않으면 none
        header=0 if is_header else None
        #is_replace가 true면 replace else면 append
        if_exists = 'replace' if is_replace else 'append'
        file_df = pd.read_csv(file_name,header=header,delimiter=delimiter)

        #window에서는 CRLF라는 텍스트 줄바꿈을 제거해줘야함
        for col in file_df.columns:
            #string이 아닌 다른 type일때 경우 오류가 발생할 수 있음
            try:
                file_df[col] = file_df[col].str.replace('\r\n','')
                self.log.info(f'{table_name}.{col}:개행문자 제거')
            except:
                continue

        self.log.info('데이터 적재 개수:'+str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        
        #db에 올리는 작업
        file_df.to_sql(
            name=table_name,
            con=engine,
            schema='public',
            if_exists=if_exists,
            index=False
        )
        