from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import task

with DAG(
    dag_id="dags_python_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
)as dag:
    
    @task(task_id = 'python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    

    @task(task_id= 'python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 받은값 : '+value1)

    @task(task_id = 'python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print("함수로 입력받은값 : "+status)

    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()
    