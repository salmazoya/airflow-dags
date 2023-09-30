from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import boto3
from botocore.exceptions import ClientError
import json
import logging

s3_client = boto3.client('s3')

logger = logging.getLogger(__name__)

with DAG(dag_id='demo', start_date=datetime(2023,9,29), schedule='0 0 * * *', catchup=False) as dag:

    hello = BashOperator(task_id='hello', bash_command='echo hello')

    @task()
    def airflow():
        try:
            print("airflow")
            json_object = 'Hello from airflow to s3'
            s3_client.put_object(
                Body=json.dumps(json_object),
                Bucket='airflow-user-posts-data',
                Key='dummy_data/airflow_msg.json'
            )
        except ClientError as e:
            logging.error(e)
    
    hello >> airflow()
