from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
import pandas as pd
import io
import boto3
import logging
import pyarrow
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# DAG 정의
dag = DAG(
    dag_id="wholesale_dynamic_data_to_redshift",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="실시간 도매 데이터 처리, Redshift적재",
    schedule_interval='30 14 * * *',
    start_date=datetime(2024, 12, 2),
    max_active_runs=1,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=1,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=True,
)

# S3에서 CSV 파일을 읽고 필요한 컬럼만 선택하여 Parquet 파일로 저장
def transform_csv_to_parquet():
    aws_conn = BaseHook.get_connection('aws_conn')
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_conn.login,
                            aws_secret_access_key=aws_conn.password)
    
    # 실행 날짜를 기반으로 파일 경로 생성
    context = get_current_context()
    execution_date = context["execution_date"]
    execution_date = execution_date.strftime('%Y%m%d')
    # execution_date = "{{ execution_date.strftime('%Y%m%d') }}"
    bucket_name = "team2-s3"
    input_file_key = f"raw_data/wholesale/dynamic_wholesale_{execution_date}.csv"
    output_file_key = f"transformed_data/wholesale/dynamic_wholesale_{execution_date}.parquet"
    logging.info(f"파일 열기 시도 - {execution_date}")
    
    try:
        # S3에서 CSV 파일 읽기
        response = s3_client.get_object(Bucket=bucket_name, Key=input_file_key)
        csv_data = response['Body'].read().decode('utf-8')
        
        # CSV 데이터를 Pandas DataFrame으로 변환
        df = pd.read_csv(io.StringIO(csv_data))
        logging.info(f"Complete Load - {execution_date}")
        
        # 필요한 컬럼만 선택
        selected_columns = ['saleDate', 'whsalCd', 'whsalName', 'large', 'largeName', 'mid', 'midName', 'small', 'smallName', 'lvCd', 'lvName', 'sanName', 'totQty', 'totAmt']
        df_selected = df[selected_columns]

        df_selected = df_selected.astype(str)
        df_selected['totQty'] = pd.to_numeric(df_selected['totQty'], errors='coerce').fillna(0).astype('int64')
        df_selected['totAmt'] = pd.to_numeric(df_selected['totAmt'], errors='coerce').fillna(0).astype('int64')
        
        # DataFrame을 Parquet 형식으로 변환
        parquet_buffer = io.BytesIO()
        df_selected.to_parquet(path=parquet_buffer, engine = 'pyarrow')

        # Parquet 파일을 S3에 저장
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_file_key,
            Body=parquet_buffer.getvalue()
        )
        logging.info(f"Complete Transform - {execution_date}")
    
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.info(f"Credentials error: {e}")
    except Exception as e:
        logging.info(f"Error occurred: {e}")

# PythonOperator를 사용한 태스크 생성
transform_task = PythonOperator(
    task_id='transform_csv_to_parquet',
    python_callable=transform_csv_to_parquet,
    dag=dag,
)

# RedshiftDataOperator를 사용한 태스크 생성
load_data = RedshiftDataOperator(
    task_id='load_data',
    aws_conn_id='aws_conn',  # Airflow에서 설정한 Redshift 연결 ID
    database="dev",  # Redshift 데이터베이스 이름
    cluster_identifier="team2-cluster",  # Redshift 클러스터 식별자
    sql="""
    COPY raw_data.wholesale
    FROM 's3://team2-s3/transformed_data/wholesale/'
    IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
    FORMAT AS PARQUET;
    """,
    region="ap-northeast-2",
    dag=dag
)

# 태스크 간 의존성 설정
transform_task >> load_data