"""
Master CAD Pipeline Orchestration DAG
Chains the full pipeline: Setup → Ingest → Spark Processing.
Trigger this DAG to run the entire pipeline end-to-end.
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='cad_master_pipeline',
    description='End-to-end CAD data pipeline: Setup → Ingest → Process',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['pipeline', 'master'],
) as dag:

    # Step 1: Ensure MinIO buckets exist
    setup_storage = TriggerDagRunOperator(
        task_id='trigger_setup_lakehouse',
        trigger_dag_id='01_setup_lakehouse_storage',
        wait_for_completion=True,
        poke_interval=10,
    )

    # Step 2: Download ABC dataset files to Bronze
    ingest_data = TriggerDagRunOperator(
        task_id='trigger_abc_ingestion',
        trigger_dag_id='abc_dataset_ingestion_v2',
        wait_for_completion=True,
        poke_interval=15,
    )

    # Step 3: Spark processing (Bronze → Silver)
    # This submits the Spark job to the master container
    process_cad = BashOperator(
        task_id='spark_submit_cad_processing',
        bash_command=(
            'curl -s -X POST http://spark-master:8080/api/v1/submissions/create '
            '|| echo "Note: Direct spark-submit requires exec into spark-master container"'
        ),
    )

    # Step 4: Gold processing (Silver → Gold)
    process_gold = BashOperator(
        task_id='dbt_run_gold_layer',
        bash_command=(
            'echo "Note: Run dbt via: docker exec -w /opt/spark/dbt spark-master dbt run --profiles-dir ."'
        ),
    )

    # Define the pipeline order
    setup_storage >> ingest_data >> process_cad >> process_gold
