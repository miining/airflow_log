from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_sensor_bash',
    start_date=pendulum.datetime(2024,3,1,tz='Asia/Seoul'),
    schedule=None,
    catchup=False
)as dag:
    sensor_task_by_poke = BashOperator(
        task_id='sesnor_task_by_poke',
        env={'FILE':'/opt/airflow/files/TbUseMonthstatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbUseMonthstatusView.csv'},
        # &&는 조건부 실행 연산자,FILE이 수행되면 뒤에 명령어를 수행해라
        bash_command=f'''echo $FILE &&
                        # -f $FILE은 $FILE경로에 FILE이 존재할때를 판단함
                        if [ -f $FILE ]; then
                            exit 0 
                        else
                            exit 1
                        fi ''',
        #텀은 20초
        poke_interval=20,
        #1분이 지나면 종료되도록하는거
        timeout=60*1,
        mode='poke',
        #프로그램이 실패하면 끝내도록하는거
        soft_fail=False

    )

    sensor_task_by_reschedule = BashSensor(
        task_id = 'sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/TbUseMonthstatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbUseMonthstatusView.csv'},
        bash_command=f'''echo $FILE &&
                        # -f $FILE은 $FILE경로에 FILE이 존재할때를 판단함
                        if [ -f $FILE ]; then
                            exit 0 
                        else
                            exit 1
                        fi ''',
        #텀은 1분
        poke_interval=60,
        #3분이 지나면 종료되도록하는거
        timeout=60*3,
        mode='poke',
        #프로그램이 실패해도 기록을 남기고 넘어감
        soft_fail=True

    )

    bash_taks=BashOperator(
        task_id='bask_task',
        env={'FILE':'/opt/airflow/files/TbUseMonthstatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbUseMonthstatusView.csv'},
        bash_command='echo "건수:'cat $FILE | wc -1' " ',
    )

    [sensor_task_by_poke,sensor_task_by_reschedule] >> bash_taks