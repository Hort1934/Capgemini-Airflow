from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
import pandas as pd
import os
import logging

# Define default_args
default_args = {
    'owner': 'marchenko',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'nyc_airbnb_etl',
    default_args=default_args,
    description='ETL pipeline for NYC Airbnb data',
    schedule_interval='@daily',  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def ingest_data(**kwargs):
        file_path = kwargs['params']['file_path']
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError(f"File {file_path} is empty.")
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_dict())

    def transform_data(**kwargs):
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(key='raw_data', task_ids='ingest_data')
        df = pd.DataFrame(raw_data)
        # Perform transformations
        df = df[df['price'] > 0]
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
        df['last_review'].fillna(df['last_review'].min(), inplace=True)
        df['reviews_per_month'].fillna(0, inplace=True)
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        # Save to CSV
        transformed_file_path = kwargs['params']['transformed_file_path']
        df.to_csv(transformed_file_path, index=False)
        kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_file_path)

    def generate_load_sql(**kwargs):
        ti = kwargs['ti']
        transformed_file_path = ti.xcom_pull(key='transformed_file_path', task_ids='transform_data')
        load_sql = f"""
        COPY airbnb_listings FROM '{transformed_file_path}' WITH (FORMAT CSV, HEADER);
        """
        ti.xcom_push(key='load_sql', value=load_sql)

    def check_data_quality(**kwargs):
        # Sample checks (implement detailed checks as needed)
        return 'pass_quality'  # or 'fail_quality'

    def fail_checks(**kwargs):
        logging.error('Data quality checks failed')
        return 'failure'

    def pass_checks(**kwargs):
        logging.info('Data quality checks passed')
        return 'success'

    def log_failure(**kwargs):
        logging.error('A task failed, logging failure details')

    start = DummyOperator(task_id='start')

    ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        provide_context=True,
        params={'file_path': 'raw/AB_NYC_2019.csv'}
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        params={'transformed_file_path': 'transformed/__MARCHENKO__AB_NYC_2019.csv'}
    )

    generate_sql = PythonOperator(
        task_id='generate_load_sql',
        python_callable=generate_load_sql,
        provide_context=True
    )

    load = PostgresOperator(
        task_id='load_data',
        sql="{{ task_instance.xcom_pull(task_ids='generate_load_sql', key='load_sql') }}",
        postgres_conn_id='marchenko_postgres_conn_id'
    )

    check_quality = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True
    )

    pass_quality = DummyOperator(task_id='pass_quality')
    fail_quality = DummyOperator(task_id='fail_quality')

    log_error = PythonOperator(
        task_id='log_failure',
        python_callable=log_failure
    )

    start >> ingest >> transform >> generate_sql >> check_quality
    check_quality >> pass_quality
    check_quality >> fail_quality
    pass_quality >> load
    fail_quality >> log_error
