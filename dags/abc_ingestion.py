"""
ABC Dataset Ingestion DAG
Downloads sample STEP files from the NYU ABC dataset archive
and uploads them to the MinIO Bronze bucket.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import requests
import logging

# MinIO Connection Info - Using the Docker internal hostname
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password"
REGION = "us-east-1"

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def download_abc_samples():
    """Download sample STEP files from ABC dataset and upload to MinIO Bronze bucket."""
    s3_res = boto3.resource(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )

    # Check existing buckets
    existing_buckets = [b.name for b in s3_res.buckets.all()]
    logging.info(f"Existing buckets: {existing_buckets}")

    target_bucket = 'bronze'

    if target_bucket not in existing_buckets:
        logging.info(f"Bucket '{target_bucket}' not found. Creating it now...")
        s3_res.create_bucket(Bucket=target_bucket)

    import glob
    
    data_dir = "/opt/airflow/data"
    step_files = glob.glob(f"{data_dir}/*.step")
    
    if not step_files:
        logging.warning("No .step files found in the Airflow data directory!")
        return

    for file_path in step_files:
        filename = file_path.split('/')[-1]
        logging.info(f"Uploading {filename} to MinIO...")

        with open(file_path, "rb") as f:
            content = f.read()

        s3_client.put_object(
            Bucket=target_bucket,
            Key=filename,
            Body=content,
        )
        logging.info(f"Successfully landed {filename} in Bronze bucket.")


with DAG(
    dag_id='abc_dataset_ingestion_v2',
    description='Downloads ABC dataset STEP files into the MinIO Bronze bucket',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['ingestion', 'bronze'],
) as dag:

    ingest_task = PythonOperator(
        task_id='download_and_upload_to_minio',
        python_callable=download_abc_samples,
    )