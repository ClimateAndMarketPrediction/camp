from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# DAG 기본 설정
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="climate_dynamic_data_to_redshift",
    default_args=default_args,
    description="날씨 동적 데이터 처리 후 Redshift 적재",
    schedule_interval='0 14 * * *',  # 매일 13:30에 실행
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
)

# Glue Job 실행 (동적 데이터 처리)
process_dynamic_data = GlueJobOperator(
    task_id='process_dynamic_data_etl',
    job_name="team2-glue-dynamic-climate",
    script_args={
        '--JOB_NAME': "team2-glue-dynamic-climate",
        '--S3_INPUT_PATH': 's3://team2-s3/raw_data/climate/2025/',
        '--S3_OUTPUT_PATH': 's3://team2-s3/transformed_data/climate/dynamic',
        # 날짜 전달 (YYYY-MM-DD 형식)
        '--DATA_DATE': "{{ execution_date.strftime('%Y-%m-%d') }}"
    },
    aws_conn_id='aws_conn',
    region_name='ap-northeast-2',
    dag=dag,
)

# Redshift로 적재
load_dynamic_data_to_redshift = RedshiftDataOperator(
    task_id='load_dynamic_data_to_redshift',
    aws_conn_id='aws_conn',
    database="dev",
    cluster_identifier="team2-cluster",
    sql="""
    COPY raw_data.climate
    FROM 's3://team2-s3/transformed_data/climate/dynamic/'
    IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
    FORMAT AS PARQUET
    """,
    region="ap-northeast-2",
    dag=dag,
)

# 중복 데이터가 있을 경우 제거 (Redshift)
remove_duplicates = RedshiftDataOperator(
    task_id='remove_duplicates',
    aws_conn_id='aws_conn',
    database="dev",
    cluster_identifier="team2-cluster",
    sql="""
    DELETE FROM raw_data.climate
    WHERE (observation_date, station_id) IN (
        SELECT observation_date, station_id
        FROM (
            SELECT observation_date, station_id,
                ROW_NUMBER() OVER (PARTITION BY observation_date, station_id ORDER BY observation_date) AS row_num
            FROM raw_data.climate
        ) subquery
        WHERE subquery.row_num > 1
    );
    """,
    region="ap-northeast-2",
    dag=dag,
)

# 태스크 의존성 설정
process_dynamic_data >> load_dynamic_data_to_redshift >> remove_duplicates
