from airflow import DAG
from airflow import models
# from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import date
import datetime


email  = ['sandtwice5@gmail.com']
owner  = 'Gnine'
CLUSTER_NAME = 'airflow-cluster'
REGION='us-central1'
PROJECT_ID= " " # ID project gcp 
PYSPARK_URI='gs://jobs_pyspark/etl_spark_energy/main.py'

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}



default_args = {
    'owner': owner,               
    'depends_on_past': False,         
    'start_date': datetime.datetime(2022, 8, 20),
    'email':email,
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
}


with DAG("ETL_energy",
         default_args = default_args,
         catchup = False,
         description='ETL process of the energy dataset',
         schedule_interval="@monthly",
        ) as dag:

        start_task = DummyOperator(task_id="start_pipeline")

        create_cluster = DataprocCreateClusterOperator(
                    task_id="create_cluster",
                    project_id=PROJECT_ID,
                    cluster_config=CLUSTER_CONFIG,
                    region=REGION,
                    cluster_name=CLUSTER_NAME,
        )

        submit_job = DataprocSubmitJobOperator(
                    task_id="extract_transform_energy", 
                    job=PYSPARK_JOB, 
                    location=REGION, 
                    project_id=PROJECT_ID
        )


        delete_cluster = DataprocDeleteClusterOperator(
                    task_id="delete_cluster", 
                    project_id=PROJECT_ID, 
                    cluster_name=CLUSTER_NAME, 
                    region=REGION
        )

        end_task = DummyOperator(task_id="end_pipeline")


        start_task >> create_cluster >> submit_job >> delete_cluster >> end_task

