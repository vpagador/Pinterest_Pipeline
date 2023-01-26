from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable

weather_dir = Variable.get("weather_dir")

default_args = {
    'owner': 'Vander',
    'depends_on_past': False,
    'email': ['vanderpagador22:gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 25),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 1, 26),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id='dag_test_dependencies',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    date_task = BashOperator(
        task_id='write_date',
        bash_command='cd ~/Desktop/get-weather-schedule && date >> date.txt',
        dag=dag)
    add_task = BashOperator(
        task_id='add_files',
        bash_command='cd ~/Desktop/get-weather-schedule && git add .',
        dag=dag)
    commit_task = BashOperator(
        task_id='commit_files',
        bash_command='cd ~/Desktop/get-weather-schedule && git commit -m "Update date"',
        dag=dag)
    push_task = BashOperator(
        task_id='push_files',
        bash_command='cd ~/Desktop/get-weather-schedule && git push',
        dag=dag)
    
    date_task >> add_task >> commit_task
    add_task >> push_task
    commit_task >> push_task