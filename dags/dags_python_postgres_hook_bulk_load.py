from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id='dags_python_postgres_hook_bulk_load',
    start_date=pendulum.datetime(tz = 'Asia/Seoul'),
    schedule=None,
    catchup=False
)as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm,file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm,file_nm)

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable= insrt_postgres,
        op_kwargs={
                    'postgres_conn_id':'conn-db-postgres-custom',
                    #db에 이런 이름의 table을 만들어나야함
                    'tbl_nm':'TbParkingStatus_bulk1',
                    #csv 파일을 파일명으로 넘겨줄거임,api_parking부분 참고해서 가져옴
                    'file_nm':'/opt/airflow/files/TbUseMonthStatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbUseMonthStatusView.csv'
                   }
    )