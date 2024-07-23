import boto3
import time
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY,AWS_REGION


def trigger_glue_job(job_name:str):

    client = boto3.client(
            'glue',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_ACCESS_KEY,
        region_name=AWS_REGION
    )


    try:
            # Trigger the Glue job
            response = client.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']
            print(f"Triggered Glue job run: {job_run_id}")
            
            while True:
                job_run = client.get_job_run(JobName=job_name, RunId=job_run_id)
                status = job_run['JobRun']['JobRunState']
                print(f"Job run status: {status}")
                
                if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                    break
                
                # Wait before polling again
                time.sleep(150)  # Adjust sleep time as needed
                
            print(f"Job run completed with status: {status}")
            
    except Exception as e:
            print(f"Error triggering or waiting for Glue job: {e}")
            exit(1)
