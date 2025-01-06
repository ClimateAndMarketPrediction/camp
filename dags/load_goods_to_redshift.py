from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

WHOLESALE_API_KEY = Variable.get("WHOLESALE_API_KEY")

def save():
    result = []
    url = f"https://at.agromarket.kr/openApi/code/good.do?serviceKey={WHOLESALE_API_KEY}&apiType=json&pageNo=1"
    res = requests.get(url).json()
    totCnt = (int(res["totCnt"]) // 1000) + 1
    result += res["data"]
    for i in range(2, totCnt+1):
        url = f"https://at.agromarket.kr/openApi/code/good.do?serviceKey={WHOLESALE_API_KEY}&apiType=json&pageNo={i}"
        res = requests.get(url).json()
        result += res["data"]
    for record in result:
        record.setdefault("small", None)
        record.setdefault("goodName", None)
        record.setdefault("gubn", None)

    csv_buffer = StringIO()  
    writer = csv.DictWriter(csv_buffer, fieldnames=result[0].keys())
    writer.writeheader()
    writer.writerows(result)


    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key='raw_data/standard_code/goods.csv',
        bucket_name='team2-s3',
        replace=True  # 동일 키가 있을 경우 덮어쓰기
    )

dag = DAG(
    dag_id="goods",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="품목 API 호출 및 변환 후 Redshift적재",
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    max_active_runs=1,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=1,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=False, 
)

save_file = PythonOperator(
    task_id='save_file',
    python_callable=save,
    dag=dag
)

load_data = RedshiftDataOperator(
        task_id='load_data',
        aws_conn_id='aws_conn',  # Airflow에서 설정한 Redshift 연결 ID
        database="dev",  # Redshift 데이터베이스 이름
        cluster_identifier="team2-cluster",  # Redshift 클러스터 식별자
        sql="""
        DROP TABLE IF EXISTS raw_data.goods;

        CREATE TABLE IF NOT EXISTS raw_data.goods(
            rn  VARCHAR(10),
            large   VARCHAR(10),
            mid   VARCHAR(10),
            largeName   VARCHAR(255),
            midName   VARCHAR(255),
            small   VARCHAR(10),
            goodName   VARCHAR(255),
            gubn   VARCHAR(10)
        );

        COPY raw_data.goods
        FROM 's3://team2-s3/raw_data/standard_code/goods.csv'
        IAM_ROLE 'arn:aws:iam::862327261051:role/team2-redshift-role'
        FORMAT AS CSV
        IGNOREHEADER 1;
        """,
        region="ap-northeast-2",
        dag=dag
    )

save_file >> load_data