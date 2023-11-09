from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
   
    dag_id="dags_py_macro", 
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    #macro를 이용한 시간 추출
    @task(task_id = 'task_using_macro',
        templates_dict = {
            'start_date' : '{{ (data_interval_end.in_timezone("Asia/Seoul")+macros.dateutil.relativedelta.relativedelta(months=-1,day=1)) | ds }}',
            'end_date' : '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1)+macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
        }
    )
    #kwagrs에 templates_dict가 key로  뒤에 딕셔너리가 value값으로 들어감
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date 없음'
            end_date = templates_dict.get('end_date') or 'end_date 없음'
            print(start_date)
            print(end_date) 
    
    #직접 시간 추출
    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        #스케쥴러 부하를 감소시키기위해 안에서 import함
        #operator안에서만 쓸거면 안에서 import 하기
        #op전 공간이나 dag선언전 공간에 양이 많을 수록 부하가 커짐
        #run하기전 오류를 검사하기때문
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1)+relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()