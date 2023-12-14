from typing import Iterable
from airflow import DAG
from airflow.utils.context import Context
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = "dags_base_branch_op",
    start_date=pendulum.datetime(2023,12,1, tz = "Asia/Seoul"),
    schedule=None,
    catchup=False
)as dag:
    #BranchOperator는 BaseBranchOperator(부모)를 상속받음
    class BranchOperator(BaseBranchOperator): 
        #choose_branch함수를 오버라이딩
        def choose_branch(self, context):
            import random

            print(context)

            item_list = ['A','B','C']
            selected_item = random.choice(item_list)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']
    #객체 생성    
    branch_operator = BranchOperator(task_id = 'python_branch_task') 

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'} #key,value
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    branch_operator >> [task_a,task_b,task_c]

        