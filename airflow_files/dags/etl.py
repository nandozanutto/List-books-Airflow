from datetime import datetime, timedelta
import os
import logging
import json
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
import boto3
from airflow.exceptions import AirflowException

# from operators import StageToRedshiftOperator
# from operators import LoadFactOperator
# from operators import LoadDimensionOperator
from operators import StartJobOperator
from operators import StartEc2Operator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'Fernando',
    'depends_on_past': False,
    'start_date': datetime(27, 9, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
    
}

def run_lambda(lambda_name):
    aws_hook = AwsGenericHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    client = boto3.client('lambda', 
                          region_name='us-west-2', 
                          aws_access_key_id=credentials.access_key, 
                          aws_secret_access_key=credentials.secret_key)
    response = client.invoke(FunctionName=lambda_name)
    if(response["StatusCode"] != 200):
        raise AirflowException(f"Failed to run lambda {lambda_name}")



dag = DAG('how_bootcamp_final',
    default_args=default_args,
    description='Final project of how bootcamp',
    schedule_interval='0 0 * * MON',
    max_active_runs=1
)



lambda_list_books = PythonOperator(
    task_id='lambda_list_books',
    python_callable=run_lambda,
    op_kwargs={'lambda_name': "testehaha"},
    dag=dag,
)

ec2_ingestion = StartEc2Operator(
    task_id='ec2_ingestion',
    dag=dag,
    instance_id="i-01f46e4512b7394e6",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    extra_params="",
)

start_dag = EmptyOperator(
    task_id='start_dag',
    dag=dag,
    retries=0,
)

finish_dag = EmptyOperator(
    task_id='finish_dag',
    dag=dag,
    retries=0,
)


clean_data = StartJobOperator(
    task_id='clean_data',
    dag=dag,
    job="how_final",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    extra_params="",
)

curate_data = StartJobOperator(
    task_id='curate_data',
    dag=dag,
    job="how_final_curated",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    extra_params="",
)



start_dag >> lambda_list_books >> clean_data
start_dag >> ec2_ingestion >> clean_data
clean_data >> curate_data >> finish_dag
