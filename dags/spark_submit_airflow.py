from datetime import datetime, timedelta
import glob
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations
BUCKET_NAME = "auuemaa-nrg"
local_data_BA = "./dags/data/EIA_BA_data/"
local_data_weather = "./dags/data/Weather/"
s3_data = "input_data/"
local_script = "./dags/scripts/spark/"
s3_script = "scripts/"
s3_script_etl = "scripts/nrg_etl.py"
s3_script_sql = "scripts/nrg_sql_queries.py"
s3_script_qc = "scripts/nrg_qc.py"
s3_output = "output_data/"

# EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    "Name": "nrg-etl",
    "LogUri":"s3://auuemaa-nrg/logs",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "Ec2KeyName": "nrg-etl",
        "Ec2SubnetId": "subnet-0ea9475e711e86bad",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",#"SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "SPOT",#"SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

#  Create Spark job steps
SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_data }}",
                "--dest=hdfs:///input_data",
            ],
        },
    },
    {
        "Name": "ETL of balancing authorities, weather and time data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_sql }}",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_etl }}",
            ],
        },
    },
    {
        "Name": "Post-processing data quality checks",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_sql }}",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script_qc }}",
            ],
        },
    },
    {
        "Name": "Move processed data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=hdfs:///output_data",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_output }}",
            ],
        },
    },
]



# helper functions
def load_local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    """
    This function loads a file from local folder into s3 bucket with
     help of boto3-like API.
     ARGUMENTS:
     * filename local path to a file to upload
     ** key bucket prefix key
     *** bucket_name name of s3 bucket
    """
    s3 = S3Hook(aws_conn_id='aws_credentials')
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

def load_files(path_to_files, path_to_s3,file_type="csv", bucket_name=BUCKET_NAME):
    """
    This function loads multiple files from a local folder into s3 bucket with
     help of boto3-like API.
     ARGUMENTS:
     * path_to_files local path to a file to upload
     ** path_to_s3 location to save in s3 bucket
     *** file_type i. e. .csv, .gz, .py
     **** bucket_name name of s3 bucket
    """
    files_list = glob.glob(path_to_files+f"*.{file_type}*")
    for filename in files_list:
        key = path_to_s3 + os.path.basename(filename)
        load_local_to_s3(filename, key, bucket_name=bucket_name)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime.now(), #datetime(2021, 11, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# ba_data_s3 = PythonOperator(
#     dag=dag,
#     task_id="ba_data_s3",
#     python_callable=load_files,
#     op_kwargs={"path_to_files": local_data_BA, "path_to_s3": s3_data+"bal_auth/",},
# )
#
# weather_data_s3 = PythonOperator(
#     dag=dag,
#     task_id="weather_data_s3",
#     python_callable=load_files,
#     op_kwargs={"path_to_files": local_data_weather, "path_to_s3": s3_data+"weather/",},
# )
#
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=load_files,
    op_kwargs={"path_to_files": local_script, "path_to_s3": s3_script, "file_type": "py"},
)

#Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr_credentials",
    dag=dag,
)

# Add steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="spark_steps",
    job_flow_id=create_emr_cluster.output,   #"{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script_etl": s3_script_etl,
        "s3_script_sql": s3_script_sql,
        "s3_script_qc": s3_script_qc,
        "s3_output": s3_output,
    },
    dag=dag,
)

# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id=create_emr_cluster.output, #"{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='spark_steps', key='return_value')[-1] }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)
#
# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id=create_emr_cluster.output, #"{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    dag=dag,
)
#
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)
#
# start_data_pipeline >> [ba_data_s3, weather_data_s3, script_to_s3] >> create_emr_cluster
# start_data_pipeline >> terminate_emr_cluster

start_data_pipeline >> script_to_s3 >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
