from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

dag = DAG(
    dag_id="retail_static_data_to_redshift",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="retail 정적 데이터 처리 후 Redshift에 적재",
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
)

# Glue Job 실행
glue_job_task = GlueJobOperator(
    task_id='glue_job_task',
    job_name='team2-glue-past-retail',
    script_args={
        '--S3_INPUT_PATH': 's3://team2-s3/raw_data/retail/static/*/*.csv',
        '--S3_OUTPUT_PATH': 's3://team2-s3/transformed_data/retail/static/',
    },
    iam_role_name='team2-glue-role',
    aws_conn_id='aws_conn',
    region_name='ap-northeast-2',
    dag=dag,
)

# Redshift COPY 명령어를 통해 데이터 적재
load_data_task = RedshiftDataOperator(
    task_id='load_data_task',
    aws_conn_id='aws_conn',
    database="dev",
    cluster_identifier="team2-cluster",
    sql="""
    COPY raw_data.retail
    FROM 's3://team2-s3/transformed_data/retail/static/'
    IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
    FORMAT AS PARQUET;
    """,
    region="ap-northeast-2",
    dag=dag,
)

# 태스크 의존성 설정
glue_job_task >> load_data_task