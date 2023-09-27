from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.exceptions import AirflowException

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from botocore.exceptions import ClientError

# from airflow.contrib.hooks.aws_hook import AwsHook

import boto3
import json
from time import sleep

class StartEc2Operator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 instance_id="",
                 aws_credentials_id="",
                 region="us-west-2",
                 extra_params="",
                 *args, **kwargs):

        super(StartEc2Operator, self).__init__(*args, **kwargs)
        self.instance_id = instance_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.extra_params = extra_params

    def execute_commands_on_linux_instances(self, client, commands):
        """Runs commands on remote linux instances
        :param client: a boto/boto3 ssm client
        :param commands: a list of strings, each one a command to execute on the instances
        :param instance_ids: a list of instance_id strings, of the instances on which to execute the command
        :return: the response from the send_command function (check the boto3 docs for ssm client.send_command() )
        """

        resp = client.send_command(
            DocumentName="AWS-RunShellScript", # One of AWS' preconfigured documents
            Parameters={'commands': commands},
            InstanceIds=[self.instance_id],

        )
        return resp

    def execute(self, context):
        aws_hook = AwsGenericHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(f"Initiating the ec2 {self.instance_id}")



        ec2 = boto3.client('ec2', 
                                region_name=self.region, 
                                aws_access_key_id=credentials.access_key, 
                                aws_secret_access_key=credentials.secret_key)


        #****************************************************starting ec2

        try:
            ec2.start_instances(InstanceIds=[self.instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                self.log.info(f"error starting {self.instance_id} ec2")
                raise AirflowException(f"error starting {self.instance_id} ec2")

            # Dry run succeeded, run start_instances without dryrun
        try:
            response = ec2.start_instances(InstanceIds=[self.instance_id], DryRun=False)
            self.log.info(response)
        except ClientError as e:
            self.log.info(e)
            raise AirflowException(f"error starting {self.instance_id} ec2")


        #*****************************************************waiting for ec2 start
        state = 'pending'
        while(state != 'running'):
            response = ec2.describe_instance_status(InstanceIds=[self.instance_id])
            for i in response['InstanceStatuses']:
                if i['InstanceId'] == self.instance_id:
                    state = i['InstanceState']['Name']
                    sleep(1)
        
        sleep(60)
        #***************************************************execute command

        ssm_client = boto3.client('ssm', 
                region_name=self.region,
                aws_access_key_id=credentials.access_key, 
                aws_secret_access_key=credentials.secret_key)
        
        script = """
        import os
        os.environ['KAGGLE_USERNAME'] = 'fernandozanutto'
        os.environ['KAGGLE_KEY'] = '9037119666621f0822210b10265b2667'

        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('joebeachcapital/amazon-books', path='/home', unzip=True)
        """
        sleep(60)
        
        commands = [f'echo """{script}"""  > /home/script.py', "python3 /home/script.py", "aws s3 cp /home/Amazon_popular_books_dataset.csv s3://how-finalproject-raw/amazon-dataset/"]
        returned = self.execute_commands_on_linux_instances(ssm_client, commands)
        command_id = returned['Command']['CommandId']


        #**********************waiting for command to be executed

        command_status = 'pending'
        while(command_status != 'Success'):
            respose_list_commands = ssm_client.list_commands(CommandId=command_id)
            for i in respose_list_commands['Commands']:
                if i['CommandId'] == command_id:
                    command_status = i['Status']
                    sleep(1)

        #******************turning off ec2

        try:
            ec2.stop_instances(InstanceIds=[self.instance_id], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                self.log.info(f"error stopping {self.instance_id} ec2")
                raise AirflowException(f"error stopping {self.instance_id} ec2")

        # Dry run succeeded, call stop_instances without dryrun
        try:
            response = ec2.stop_instances(InstanceIds=[self.instance_id], DryRun=False)
            self.log.info(response)
        except ClientError as e:
            self.log.info(e)
            raise AirflowException(f"error stopping {self.instance_id} ec2")

        self.log.info("Sucess in executing ec2")