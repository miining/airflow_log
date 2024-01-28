from operators.seoul_api_to_csv_op import SeoulApiToCsvOp
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_parking',
    schedule=None,
    start_date=pendulum.datetime(2023,1,1,tz='Asia/Seoul'),
    catchup=False
)as dag:
    '''서울시 한강공원 월별 주차장 이용현황'''
    tb_use_month_status = SeoulApiToCsvOp(
        task_id = 'tb_use_month_status',
        dataset_nm='TbUseMonthstatusView',
        path='/opt/airflow/files/TbUseMonthstatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='TbUseMonthStatusView.csv'
    )

    tb_use_month_status