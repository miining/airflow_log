from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    regist_t2 = PythonOperator(
        task_id = 'regist_t2',
        python_callable=regist2,
        op_args=['min','kr','ulsan'],
        op_kwargs={'email':'aaa@naver.com','phone':'010'}
    )
    
regist_t2