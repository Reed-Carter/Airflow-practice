from datetime import timedelta, datetime
import random

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago # handy scheduling tool

def print_hello():
    with open('/opt/airflow/dags/code_review.txt') as f:
    #with open('./dsa-airflow/dags/code_review.txt') as f:
        text = f.read()
        return (f"Hows it goin, {text}?")   

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def select_random():
    return (f"The apple your going to eat today is... {str(random.choice(APPLES))}!")

# We can pass in DAG arguments using a default args dict. All these could be passed directly to the DAG as well.
default_args = {
    'start_date': days_ago(2), # The start date for DAG running. This function allows us to set the start date to two days ago
    'schedule_interval': timedelta(days=1), # How often our DAG will run. After the start_date, airflow waits for the schedule_interval to pass then triggers the DAG run
    'retries': 1, # How many times to retry in case of failure
    'retry_delay': timedelta(minutes=2), # How long to wait before retrying
}

with DAG(
    'code_review1654', # a unique name for our DAG
    description='A DAG using a sequence of toy actions to practice using Airflow"', # a description of our DAG
    default_args=default_args, # pass in the default args.
) as dag:

    Name_task = BashOperator(
        task_id='print_Reed',
        bash_command='echo Reed > code_review.txt'
    )

    greeting_task = PythonOperator(
        task_id="greeting",
        python_callable=print_hello
    )

    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo "picking three random apples"'
    )

    select_task1 = PythonOperator(
        task_id="select_task1",
        python_callable=select_random
    )

    select_task2 = PythonOperator(
        task_id="select_task2",
        python_callable=select_random
    )

    select_task3 = PythonOperator(
        task_id="select_task3",
        python_callable=select_random
    )
    
    dummy_task = DummyOperator(
        task_id='dummy'
    )



    # set the task order
    Name_task >> greeting_task >> echo_task >> [select_task1, select_task2, select_task3] >> dummy_task