from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    #원래 코드는 dag_id="example_bash_operator",  
    #airflow페이지에서 나타나는 이름들임, 파이썬 파일명과 동일시시켜주면 좋음!!
    #아래처럼 파일이름으로 바꿔줬음
    dag_id="dags_bash_operator", 
    schedule="0 0 * * *",
    #UTC는 세계표준시, 서울시간으로 바꿔서 사용하기
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    #과거 날짜에는 돌리지 않도록 함
    catchup=False,
) as dag:
        #task객체명
    bash_t1 = BashOperator(
        #airflow페이지 드가서 grap들어가면 볼 수 있음
        #객체명과 task id는 동일시 시키는게 보기 좋음
        task_id="bash_t1",
        #어떤 shell script를 실행시킬지
        bash_command="echo whoami",
    )
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo &HOSTNAME",
    )
        
    bash_t1 >> bash_t2
        
        
