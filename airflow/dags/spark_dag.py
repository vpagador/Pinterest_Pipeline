from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from spark_scripts.spark_read_data import Spark_Clean

job = Spark_Clean() 


default_args = {
    'owner': 'Vander',
    'depends_on_past': False,
    'email': ['vanderpagador22:gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2021, 1, 1),
}

with DAG(dag_id='spark_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         ) as dag:
    # Python Operator calls the spark clean job
    spark_read_data = PythonOperator(
        task_id = 'spark_read_data',
        python_callable= job.spark_job,
        dag=dag
    )

