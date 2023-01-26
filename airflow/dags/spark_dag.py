from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, "/home/van28/Desktop/AiCore/Pinterest_Project/Project/Pinterest_Pipeline/spark_scripts")

from spark_scripts.spark_read_data import Spark_Clean

job = Spark_Clean()

default_args = {
    'owner': 'Vander',
    'depends_on_past': False,
    'email': ['vanderpagador22:gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2021, 1, 24),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2021, 1, 24),
}

with DAG(dag_id='spark_dag',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    spark_read_data = PythonOperator(
        task_id = 'spark_read_data',
        python_callable= job.spark_job,
        dag=dag)