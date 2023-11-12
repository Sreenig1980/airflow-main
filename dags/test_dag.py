# import functions from Airflow 

from airflow import DAG
from datetime import datetime
import pandas as pd
import json
import requests
from airflow.operators.bash_Operator import bashOperator
from airflow.operators.python import pythonOperator

# setup default arguments 

default_args = default_args {
  'author':'sreenivasulu',
  'description':'this is my first Dag craeation with simple code',
  'start_date':'datetime(2023,11,01),
  'schedule_interval':@daily
}

# define function
def print_function(name):
  print("my name is + name ")

# DAG setup

dag1 = DAG(
  'dag_id' = 'sample_dag',
  'default_args' = default_args,
  'author' = 'sreenivasulu',
  'description' = 'this is my first DAG creation',
  'schedule_interval'='0 9 * * *',
  'catchup' = False,
)

# Task setup

print_task1 = pythonOperator(
  python_callable = print_function,
  op_kwargs={'name':'Sreenivasulu Gaddam'},
  dag = dag1
)



  
