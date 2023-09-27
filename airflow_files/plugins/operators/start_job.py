from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.exceptions import AirflowException

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.aws_hook import AwsHook

import boto3
import json
from time import sleep

class StartJobOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 job="",
                 aws_credentials_id="",
                 region="us-west-2",
                 extra_params="",
                 *args, **kwargs):

        super(StartJobOperator, self).__init__(*args, **kwargs)
        self.job = job
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        aws_hook = AwsGenericHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(f"Initiating the job {self.job}")


        client = boto3.client('glue', 
                    region_name=self.region, 
                    aws_access_key_id=credentials.access_key,
                    aws_secret_access_key=credentials.secret_key,
                    )

        response = client.start_job_run(
            JobName=self.job
        )

        if(response["ResponseMetadata"]["HTTPStatusCode"] != 200):
            self.log.info(f"Starting job {self.job} failed")
            raise AirflowException(f"Starting job {self.job} failed")

        error_states = ["FAILED", "STOPPED", "TIMEOUT", "ERROR"]
        final_state = 'RUNNING'
        
        while(final_state != "SUCCEEDED" and final_state not in error_states):
            job_run = client.get_job_run(
                JobName=self.job,
                RunId=response["JobRunId"],
                PredecessorsIncluded=False
            )
            final_state = job_run["JobRun"]["JobRunState"]
            sleep(1)
        
        if(final_state in error_states):
            self.log.info(f"job {self.job} failed during execution")
            raise AirflowException(f"job {self.job} failed during execution")

        # task_instance = context['task_instance']
        # if self.job == 'how_final':
        #     task_instance.xcom_push('my_id', 'job123')
        # else:
        #     self.log.info(f"result: {task_instance.xcom_pull('Start_job', key='my_id')}")
        # self.log.info(response["JobRunId"])
        # self.log.info(f"started the job {self.job}")
        # self.log.info(json.dumps(response, indent=4, sort_keys=True, default=str))
        # return response["JobRunId"]

        self.log.info(f"job {self.job} sucessfully executed")
