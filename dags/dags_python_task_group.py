from airflow.decorators import task_group
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='dags_python_task_group',
    schedule=None,
    start_date=pendulum.datetime(2023,12,1,tz="Asia/Seoul"),
    catchup=False
)as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데코레이터를 이용한 첫번째 그룹'''
        #docstring으로 tooltip으로 ui에 표시됨

        @task(task_id = 'inner_function1') #해당 task의 고유식별자임
        def inner_func1(**kwargs): #airflow dag에서 생성한 task를 생성하는 역할
            print('첫번째 taskgroup내 첫번재 task')

        inner_function2 = PythonOperator(
            task_id = 'inner_func2', #task 고유식별자
            python_callable=inner_func,
            op_kwargs={'msg':'첫번째 taskgroup 내 두번째 task'}
        )

        #등록된taks의 인스턴스임
        inner_func1() >> inner_function2
        #inner_function2()이렇게 써도 상관은 없음

    with TaskGroup(group_id = 'second_group',tooltip='두번째 그룹') as group_2:
        '''여기에 적은 decostring은 표시되지 않음'''
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print("두번째 taskgroup내 첫번째task")
        
        inner_function2 = PythonOperator(
            task_id = 'inner_func2', #task의 고유식별자
            python_callable=inner_func,
            op_kwargs={'msg':'두번째 taskgroup 내 두번째 task'}
        )

        inner_func1() >> inner_function2

    group_1() >> group_2

    




