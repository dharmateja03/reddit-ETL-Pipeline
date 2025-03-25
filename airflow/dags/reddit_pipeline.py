from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 24),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

dag = DAG(
    'reddit_analytics_pipeline',
    default_args=default_args,
    schedule='@daily'
)

extract_reddit = BashOperator(
    task_id='extract_reddit',
    bash_command='python3 /Users/dharmatejasamudrala/reddit-etl/airflow/extraction/extract-from-reddit.py',
    dag=dag
)

load_to_s3 = BashOperator(
    task_id='load_to_s3',
    bash_command='python3 /Users/dharmatejasamudrala/reddit-etl/airflow/extraction/upload_to_s3.py',
    dag=dag
)

load_to_redshift = BashOperator(
    task_id='load_to_redshift',
    bash_command='python3 /Users/dharmatejasamudrala/reddit-etl/airflow/extraction/s3_to_redshift.py',
    dag=dag
)

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /Users/dharmatejasamudrala/reddit-etl/dbt/reddit_dbt && dbt run',
    dag=dag
)

extract_reddit >> load_to_s3 >> load_to_redshift >> run_dbt