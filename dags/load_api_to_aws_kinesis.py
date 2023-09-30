from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import Connection
from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException


## External package
import json
from datetime import datetime
import boto3
import logging

logger = logging.getLogger(__name__)

# kinesis_client = boto3.client('kinesis')   

## Setting up incremental user id for next call
def _set_api_user_id(api_user_id):
    logger.info(f'type:: {type(api_user_id)} and api_user_id:: {api_user_id}')
    if api_user_id == -1 or api_user_id == 10:
        Variable.set(key="api_user_id", value=1)  
    else:
        Variable.set(key="api_user_id", value=int(api_user_id)+1) 
    return f"Latest api user id {int(Variable.get(key='api_user_id'))} sucessfully"


def _process_user_posts(ti):
    new_api_user_id = Variable.get("api_user_id")
    stream_name = "user-posts-data-stream"
    
    logger.info(f'type:: {type(new_api_user_id)} and new_api_user_id:: {new_api_user_id}')
    user_posts = []
    ## POSTS END POINT RESPONSE
    # with open('/opt/airflow/data/user_posts.json') as json_object:
    #     user_posts = json.load(json_object)   
    user_posts = ti.xcom_pull(task_ids='extract_user_posts')
    logger.info(f'api data||user_posts:: {user_posts}')
    
    # Writing data one by one to kinesis data stream
    for user_post in user_posts:
        response = kinesis_client.put_record(
            StreamName = stream_name,
            Data=json.dumps(user_post)+'\n',
            PartitionKey=str(user_post['userId']),
            SequenceNumberForOrdering=str(user_post['id']-1)
        )
        logger.info(f"Produced (Kinesis Data Stream) records {response['SequenceNumber']} to Shard {response['ShardId']}, status code {response['ResponseMetadata']['HTTPStatusCode']} and retry attempts count {response['ResponseMetadata']['RetryAttempts']}")
   
    return f'Total {len(user_posts)} posts with user id {new_api_user_id} has been written into kinesis stream `{stream_name}` '

def list_connections():
    conn =  None
    try:
        conn = BaseHook.get_connection("api_post_conn_id")
    except AirflowNotFoundException as airflow_error:
        logger.info(f'AIRFLOW ERROR:: {airflow_error}')
        logger.info('Creating new_connection')
        conn = Connection(
            conn_id='api_post_conn_id',
            conn_type='http',
            host='https://jsonplaceholder.typicode.com/posts'
        ) 
        #create a connection object
        session = settings.Session() # get the session
        session.add(conn)
        session.commit() # it will insert the connection object programmatically.
        logger.info('Creating new_connection done')
    except Exception as e:
        logger.info(f'Other ERROR:: {airflow_error}')
    finally:
        return conn
    
with DAG(dag_id='load_api_aws_kinesis', default_args={'owner': 'Sovan'}, tags=["api data load to s3"], start_date=datetime(2023,9,24), schedule='@daily', catchup=False):

    create_if_connection_not_exists = PythonOperator(
        task_id='create_if_connection_not_exists',
        python_callable=list_connections
    )
    get_api_user_id = PythonOperator(
        task_id = 'get_api_user_id',
        python_callable = _set_api_user_id,
        op_args=[int(Variable.get("api_user_id", default_var=-1))]
    ) 

    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'api_post_conn_id',
        endpoint = f"/posts?userId={int(Variable.get(key='api_user_id', default_var=-1))}"
    )

    extract_user_posts = SimpleHttpOperator(
        task_id = 'extract_user_posts',
        http_conn_id = 'api_post_conn_id',
        endpoint = f"/posts?userId={int(Variable.get(key='api_user_id', default_var=-1))}",
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    # process_user_posts = PythonOperator(
    #    task_id = 'process_user_posts',
    #    python_callable = _process_user_posts
    # )

    create_if_connection_not_exists >> get_api_user_id >> is_api_available >> extract_user_posts 
