# import functions from Airflow 

from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from datetime import datetime, date
import requests
import json
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

# setup default arguments 

default_args = {
  'owner': 'sreenivasulu',
  'description': 'this is my first Dag craeation with simple code'

}

# define function
def print_function(name):
  print("my name is" + name)

# DAG setup

with DAG(
  dag_id = 'sample_dag_v1',
  default_args=default_args,
  description='this is my first DAG creation',
  start_date=datetime(2023, 11, 1),
  schedule_interval='@daily',
  catchup=True,
) as dag1:

# Task setup

  print_task1 = PythonOperator(
    task_id = 'first_task_v1',
    python_callable = print_function,
    op_kwargs={'name':'Sreenivasulu Gaddam'},
    dag = dag1
  )

