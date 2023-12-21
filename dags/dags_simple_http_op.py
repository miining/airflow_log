from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    dag_id="dags_simple_http_op",
    schedule=None,
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    river_parking_info = SimpleHttpOperator(
        task_id = 'river_parking_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/TbUseMonthstatus/1/5/',
        method='GET',
        headers={
            'Connect-Type' : 'application/json',
            'charset' : 'utf-8',
            'Accept' : '*/*'
        }
    )

    @task(task_id = 'python2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        res = ti.xcom_pull(task_ids = 'river_parking_info')
        import json
        from pprint import pprint

        pprint(json.loads(res))

    river_parking_info >> python_2()


    
