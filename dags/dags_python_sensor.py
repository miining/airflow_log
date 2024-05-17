from airflow import DAG
from airflow.sensors.python import PythonSensor
import pendulum
from airflow.hooks.base import BaseHook

with DAG(
    dag_id="dags_python_sensor",
    start_date=pendulum.datetime(2024,5,1,tz='Asia/Seoul'),
    schedule= None,
    catchup=False
)as dag:
    #python_senosr가 수행할 함수임
    def check_api_update(http_conn_id,endpoint,base_dt_col,**kwargs):
        import requests
        import json
        from dateutil import relativedelta
        connection = BaseHook.get_connection(http_conn_id) #airflow ui에 입력한 정보들을 사용
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100'
        response = requests.get(url)

        contetns = json.loads(response.text)
        key_nm = list(contetns.keys())[0]
        row_data = contetns.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = last_dt[:10] #2024/05/11 이렇게 10자리만 가져오겠다는 뜻
        last_date = last_date.replace('.','-').replace('/','-')# -로 다 바꿈

        try:
            pendulum.from_format(last_date,'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col}칼럼은 YYYY.MM.DD또는 YYYY/MM/DD형태가 아닙니다.')

        today_ymd = kwargs.get("data_interval_end").in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
        if last_date >= today_ymd:
            print(f'생성 확인(배치 날짜: {today_ymd} / API Last 날짜 {last_date})')
            return True
        else:
            print(f'update 미완료(배치 날짜:{today_ymd}) / API Last 날짜 {last_date})')
            return False
        
    sensor_task = PythonSensor(
        task_id = 'sensor_task',
        python_callable= check_api_update,
        op_kwargs={
            'http_conn_id':'openapi_seoul_go.kr',
            'endpoint':'{{var.value.apikey_openapi_seoul_go_kr}}/json/',
            'base_dt_col':'S_DT'
        },
        poke_interval = 600, #10분
        mode = 'reschedule'
    )        


