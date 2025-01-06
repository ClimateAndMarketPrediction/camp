from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

dag = DAG(
    dag_id="region",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="등급 API 호출 및 변환 후 Redshift적재",
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    max_active_runs=1,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=1,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=False, 
)


load_data = RedshiftDataOperator(
        task_id='load_data',
        aws_conn_id='aws_conn',  # Airflow에서 설정한 Redshift 연결 ID
        database="dev",  # Redshift 데이터베이스 이름
        cluster_identifier="team2-cluster",  # Redshift 클러스터 식별자
        sql="""
        DROP TABLE IF EXISTS raw_data.region;

        CREATE TABLE IF NOT EXISTS raw_data.region(
            CITY VARCHAR(100),
            STN_ID VARCHAR(100),
            STN_NAME VARCHAR(100)

        );

        COPY raw_data.region
        FROM 's3://team2-s3/transformed_data/standard_code/region.json'
        IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
        FORMAT AS CSV;
        """,
        region="ap-northeast-2",
        dag=dag
    )

save_file >> load_data