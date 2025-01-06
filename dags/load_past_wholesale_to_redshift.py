from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

dag = DAG(
    dag_id="wholesale_static_data_to_redshift",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="과거 도매데이터 처리 후후 Redshift적재",
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    max_active_runs=1,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=1,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=False, 
)
    
# GlueJobOperator를 사용한 태스크 생성
glue_job_task = GlueJobOperator(
    task_id='glue_job_task',
    job_name='team2-glue-past-wholesale',  # Glue Job 이름
    script_args={
        '--S3_INPUT_PATH': 's3://team2-s3/raw_data/past_wholesale/*.csv',        # Glue 스크립트에 전달할 인자
        '--S3_OUTPUT_PATH': 's3://team2-s3/transformed_data/past_wholesale/'
    },
    aws_conn_id='aws_conn',     # Airflow 연결 ID
    region_name='ap-northeast-2',       # AWS 리전
    dag=dag
)

load_data = RedshiftDataOperator(
        task_id='load_data',
        aws_conn_id='aws_conn',  # Airflow에서 설정한 Redshift 연결 ID
        database="dev",  # Redshift 데이터베이스 이름
        cluster_identifier="team2-cluster",  # Redshift 클러스터 식별자
        sql="""
        COPY raw_data.wholesale
        FROM 's3://team2-s3/transformed_data/past_wholesale/'
        IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
        FORMAT AS PARQUET;
        """,
        region="ap-northeast-2",
        dag=dag
    )

glue_job_task >> load_data