"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import stroll.crawl.crawl as crawl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def crawl_command():
    crawl.main()

def rag_command():
    pass

def save_result():
    pass


crawl_and_rag_dag = DAG("crawl_and_rag", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = PythonOperator(task_id="crawl", python_callable = crawl_command, dag=crawl_and_rag_dag)

t2 = PythonOperator(task_id="rag", python_callable = rag_command, retries=1, dag=crawl_and_rag_dag)

t3 = PythonOperator(task_id="save_result", python_callable = save_result, retries=1, dag=crawl_and_rag_dag)


t2.set_upstream(t1)
t3.set_upstream(t2)
