from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.operators.python import get_current_context
import boto3
import requests


with DAG(
    dag_id="wholesale_static_data",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="ETL 작업을 월 단위로 수행하는 DAG",
    schedule_interval="0 0 1 * *",  
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2024, 12, 1),  
    max_active_runs=3,  # 동시에 실행되는 DAG 인스턴스를 1개로 제한
    concurrency=2,      # 동시에 실행되는 태스크를 1개로 제한
    catchup=True, 
)as dag:
    
    @task
    def get_file_id():
        context = get_current_context()
        execution_date = context["execution_date"]
        ym = execution_date.strftime("%Y%m")
        ym_str = ym[0:4]+"년 "+ym[4:6]+"월"
        file_information_url = 'https://kadx.co.kr:9090/t/41a41690-595f-11eb-8fb9-7f0e72446d5e'
        data_list = requests.get(file_information_url).json()
        file_id = ''
        for data in data_list:
            if ym_str in data['fileName']:
                file_id = data['fileID']              
                break
        return {"file_id": file_id, "ym": ym}
    
    @task
    def download(file_data):
        file_id = file_data["file_id"]
        ym = file_data["ym"]
        
        if not file_id:
            print(f"# {ym}에 해당하는 파일이 미존재재")
            return
        url = 'https://kadx.co.kr:9090/dl/'+file_id
        res = requests.get(url)
        if res.status_code == 200:
            file_name = 'raw_data/past_wholesale/static_wholesale_'+ym+'.csv'
            bucket_name = 'team2-s3'
            aws_conn = BaseHook.get_connection('aws_conn')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_conn.login,
                aws_secret_access_key=aws_conn.password
            )
            # 파일 업로드
            try:
                s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=res.content)
                print(f"'{file_name}' -> '{bucket_name}/{file_name}' 업로드 완료")
            except Exception as e:
                print(f"Error: {e}")
        else:
            print(f"# 파일 다운로드 실패: {res.status_code}")


    file_data = get_file_id()
    download(file_data)
