from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

import boto3
import pendulum

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def fetch_s3_file(bucket, key):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f'/data/{key}')


@dag(
    schedule_interval=None, 
    start_date=pendulum.parse('2023-02-20')
)
def get_data_from_s3():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={
                'bucket': 'sprint6', 
                'key': key
            },
        ) for key in bucket_files
    ]

    start >> fetch_tasks >> end

_ = get_data_from_s3() 