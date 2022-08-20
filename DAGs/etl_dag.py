from airflow import DAG
import datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocSubmitJobOperator, ClusterGenerator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import date, trigger_rule

GOOGLE_CONN_ID = "google_cloud_default"
email  = ['sandtwice5@gmail.com']
owner  = 'Gnine'
CLUSTER_NAME = 'airflow-spark-cluster'
REGION='us-central1'
PROJECT_ID = Variable.get("project")
BUCKET_NAME = 'Data_Lake'
PYSPARK_URI_1='gs://Data_Lake/jobs_pyspark/etl_spark_energy.py'
PYSPARK_URI_2='gs://Data_Lake/jobs_pyspark/etl_spark_energy_source.py'
PYSPARK_URI_3='gs://Data_Lake/jobs_pyspark/etl_spark_industries.py'
PYSPARK_URI_4='gs://Data_Lake/jobs_pyspark/load_datawarehouse.py'


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=BUCKET_NAME,
).make()

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


with DAG("pipeline_etl",
         default_args = default_args,
         catchup = False,
         description='ETL process automatized airflow',
         schedule_interval="@once",
         user_defined_macros={"project": PROJECT_ID}
        ) as dag:

        start_task = DummyOperator(task_id="start_pipeline")

        create_schema = 

        create_cluster = DataprocCreateClusterOperator(
                    task_id="create_dataproc",
                    project_id= '{{ project }}',
                    cluster_name="spark_cluster-{{ ds_nodash }}"
                    region=REGION,
                    cluster_name=CLUSTER_NAME,
        )

        submit_job_energy = DataprocSubmitJobOperator(
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

        t_join = DummyOperator(task_id='t_join', dag=dag, trigger_rule='all_success')

        start_pipeline >> create_dataset >> create_cluster

        create_cluster >> job_1 >> t_join
        create_cluster >> job_2 >> t_join
        create_cluster >> job_3 >> t_join

        t_join >> submit_job_bigquery >> delete_cluster >> end_task


