"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from stroll.crawl.crawl import crawl
from stroll.crawl.convert_address import convert_address
from stroll.crawl.send_to_stroll_api import send_place_to_api
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

def rag_command():
    pass

def save_result():
    pass


crawl_and_rag_dag = DAG("crawl_and_rag", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
crawl_task = PythonOperator(task_id="crawl", python_callable = crawl, dag=crawl_and_rag_dag)
# address_conversion_task = PythonOperator(task_id="address_conversion", python_callable = convert_address, dag=crawl_and_rag_dag)
# send_to_stroll_api_task = PythonOperator(task_id="send_to_stroll_api", python_callable = send_to_stroll_api, dag=crawl_and_rag_dag)


t2 = PythonOperator(task_id="rag", python_callable = rag_command, retries=1, dag=crawl_and_rag_dag)

t3 = PythonOperator(task_id="save_result", python_callable = save_result, retries=1, dag=crawl_and_rag_dag)


# address_conversion_task.set_uptream(crawl_task)
# send_to_stroll_api_task.set_uptream(address_conversion_task)
