from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models import Variable

# DAG 기본 설정
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    dag_id="climate_static_data_to_redshift",
    default_args=default_args,
    description="날씨 데이터 처리 후 Redshift 적재",
    schedule_interval='@once',
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=1,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=False,
)

# GlueJobOperator를 사용한 태스크 생성
process_climate_data = GlueJobOperator(
    task_id='process_climate_static_data_etl',
    job_name="team2-glue-climate",  # 직접 설정한 Glue Job 이름
    script_args={
        '--JOB_NAME': "team2-glue-climate",  # Glue 작업에 JOB_NAME 전달
        # '--S3_INPUT_PATH': Variable.get("climate_static_input_path"),  # Variable에서 입력 경로 읽기
        '--S3_INPUT_PATH': 's3://team2-s3/raw_data/climate/{2020,2021,2022,2023,2024}/**/*.csv',
        # '--S3_OUTPUT_PATH': Variable.get("climate_static_output_path"),  # Variable에서 출력 경로 읽기
        '--S3_OUTPUT_PATH': 's3://team2-s3/transformed_data/climate/',
    },
    aws_conn_id='aws_conn',  # Airflow 연결 ID
    region_name='ap-northeast-2',  # AWS 리전
    dag=dag
)

# RedshiftDataOperator를 사용한 태스크 생성
load_climate_data = RedshiftDataOperator(
    task_id='load_climate_data_to_redshift',
    aws_conn_id='aws_conn',  # Airflow에서 설정한 Redshift 연결 ID
    database="dev",  # Redshift 데이터베이스 이름
    cluster_identifier="team2-cluster",  # Redshift 클러스터 식별자
    sql=f"""
    COPY raw_data.climate
    FROM 's3://team2-s3/transformed_data/climate/'
    IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
    FORMAT AS PARQUET;
    """,
    region="ap-northeast-2",
    dag=dag
)

# 태스크 의존성 설정
process_climate_data >> load_climate_data
