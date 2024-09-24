# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 20:01:40 2024

@author: dalyuki
"""
   
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'dalyuki',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1, 7, 0, 0),
    'email': ['test@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    
}
 
dag = DAG(
    dag_id='daily_actions_counter_dag',
    schedule_interval= '0 7 * * *',
    catchup=False, 
    default_args=default_args
)
with dag:
    run_script = BashOperator(
        task_id='run_script',
        bash_command='python action_counter.py'        
    )

 