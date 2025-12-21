from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

with DAG('debug_dag_test', schedule='@once', start_date=pendulum.today('UTC'), catchup=False) as dag:
    EmptyOperator(task_id='hello')