from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from get_weather_schedule.weather import Weather

default_args = {
    'owner': 'Vander',
    'depends_on_past': False,
    'email': ['vanderpagador22:gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 24),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 1, 24),
}

with DAG(dag_id='weather-dag',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    say_hi_task = PythonOperator(
        task_id = 'say_hi',
        python_callable= Weather.say_hi,
        dag=dag
)
    get_info_task = PythonOperator(
        task_id = 'get_info',
        python_callable= Weather.get_info,
        dag=dag
    )
    export_json_task = PythonOperator(
        task_id = 'export_json',
        python_callable= Weather.export_json,
        dag=dag
    )
    
    say_hi_task >> get_info_task >> export_json_task 
    