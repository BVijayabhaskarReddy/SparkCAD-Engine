"""
Lakehouse Setup DAG
Creates the Bronze, Silver, and Gold buckets in MinIO
if they don't already exist. Run this once before other DAGs.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
from botocore.exceptions import ClientError

# Configuration for MinIO
# 'minio' is the service name we defined in docker-compose
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_REGION = "us-east-1"

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def create_medallion_buckets():
    """Connects to MinIO and ensures the Bronze, Silver, and Gold buckets exist."""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

    buckets = ['bronze', 'silver', 'gold']

    for bucket_name in buckets:
        try:
            s3.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' already exists.")
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                s3.create_bucket(Bucket=bucket_name)
                logging.info(f"Successfully created bucket: '{bucket_name}'")
            else:
                logging.error(f"Error checking bucket '{bucket_name}': {e}")
                raise


# Define the Airflow DAG
with DAG(
    dag_id='01_setup_lakehouse_storage',
    description='Initializes the Bronze, Silver, and Gold buckets in MinIO',
    default_args=default_args,
    schedule='@once',  # Run it once to setup the environment
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup'],
) as dag:

    setup_task = PythonOperator(
        task_id='initialize_minio_buckets',
        python_callable=create_medallion_buckets,
    )