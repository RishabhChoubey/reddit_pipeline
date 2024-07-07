import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.glue_trigger import trigger_glue_job

default_args = {
    'owner': 'Rishabh choubey',
    'start_date': datetime(2024, 7, 3)
}

dt = '{{ds}}'

file_postfix = datetime.now().strftime("%Y%m%d")
print(file_postfix)
dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'engineering',
        'time_filter': dt,
        'limit': 1000
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

trig_glue = PythonOperator(
    task_id='trig_glue',
    python_callable=trigger_glue_job,
     op_kwargs={
        'job_name': 'api_glue_process2',

    },
    dag=dag
)

extract >> upload_s3 >> trig_glue