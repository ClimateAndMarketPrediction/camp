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
    dag_id="retail_dynamic_data_to_redshift",
    default_args=default_args,
    description="소매 동적 데이터 처리 후 Redshift 적재",
    schedule_interval='40 13 * * *',  # 매일 13:40에 실행
    start_date=datetime(2024, 12, 31),
    max_active_runs=1,
    concurrency=1,
    catchup=True,
)

# Glue Job 실행 (동적 데이터 처리)
process_dynamic_data = GlueJobOperator(
    task_id='process_dynamic_data_etl',
    job_name="team2-glue-dynamic-retail",
    script_args={
        '--JOB_NAME': "team2-glue-dynamic-retail",
        '--S3_INPUT_PATH': 's3://team2-s3/raw_data/retail/2025/',
        '--S3_OUTPUT_PATH': 's3://team2-s3/transformed_data/retail/dynamic',
        '--DATA_DATE': "{{ macros.ds_add(ds, 1) }}" 
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
    COPY raw_data.retail
    FROM 's3://team2-s3/transformed_data/retail/dynamic/'
    IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
    FORMAT AS PARQUET
    """,
    region="ap-northeast-2",
    dag=dag,
)

# 중복 데이터가 있을 경우 제거 (Redshift)
#remove_duplicates = RedshiftDataOperator(
#    task_id='remove_duplicates',
#    aws_conn_id='aws_conn',
#    database="dev",
#    cluster_identifier="team2-cluster",
#    sql="""
#    DELETE FROM raw_data.retail
#    WHERE (saledate, midname, smallname, lv) IN (
#        SELECT saledate, midname, smallname, lv
#        FROM (
#            SELECT saledate, midname, smallname, lv
#                ROW_NUMBER() OVER (PARTITION BY saledate, midname, smallname, lv ORDER BY saledate) AS row_num
#            FROM raw_data.retail
#        ) subquery
#        WHERE subquery.row_num > 1
#    );
#    """,
#    region="ap-northeast-2",
#    dag=dag,
#)

# 태스크 의존성 설정
process_dynamic_data >> load_dynamic_data_to_redshift
