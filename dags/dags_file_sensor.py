from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2024,3,1,tz='Asia/Seoul'),
    schedule=None,
    catchup=False
)as dag:
    tbUseMonthstatusView_sensor = FileSensor(
        task_id = 'tbUseMonthstatusView_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='TbUseMonthstatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbUseMonthstatusView.csv',
        recursive=False,
        poke_interval=30,
        timeout=60*2, #2분
        mode ='reschedule'
    )
    