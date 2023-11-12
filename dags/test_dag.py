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
  'author': 'sreenivasulu',
  'description': 'this is my first Dag craeation with simple code',
  'start_date': datetime(2022, 1, 21)
},

# define function
def print_function(name):
  print("my name is" + name)

# DAG setup

dag1 = DAG(
  dag_id = 'sample_dag',
  default_args = default_args,
  author = 'sreenivasulu',
  description = 'this is my first DAG creation',
  schedule_interval='0 9 * * *',
  catchup = False,
)

# Task setup

print_task1 = pythonOperator(
  python_callable = print_function,
  op_kwargs={'name':'Sreenivasulu Gaddam'},
  dag = dag1
)



  
