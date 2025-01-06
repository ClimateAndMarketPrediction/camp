from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import boto3
import requests
import json
import pandas as pd
from io import StringIO

# 도매시장 코드
market_codes = [
    210001, 210009, 380201, 370101, 320201, 320101, 320301, 210005, 110001, 110008,
    310101, 310401, 310901, 311201, 230001, 230003, 360301, 240001, 240004, 350402,
    350301, 350101, 250001, 250003, 330101, 340101, 330201, 370401, 371501, 220001,
    380401, 380101, 380303
]

with DAG(
    dag_id="wholesale_dynamic_data_to_s3",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="실시간 도매 데이터 수집, S3 저장",
    schedule_interval="0 14 * * *",
    start_date=datetime(2024, 12, 1),
    max_active_runs=3,
    concurrency=2,
    catchup=True,
) as dag:

    @task
    def collect_data():
        """API에서 데이터를 수집하고 페이지네이션을 처리"""
        context = get_current_context()
        execution_date = context["execution_date"]
        date = execution_date.strftime('%Y%m%d')
        api_key = Variable.get("WHOLESALE_API_KEY")

        all_data = []
        for market_code in market_codes:
            page_no = 1
            while True:
                url = f"https://at.agromarket.kr/openApi/price/sale.do?serviceKey={api_key}&apiType=json&pageNo={page_no}&whsalCd={market_code}&saleDate={date}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    print(f"Market Code: {market_code}, Page: {page_no}, Response Status: {data.get('status')}")  # 디버깅 로그

                    if data.get("status") != "success":
                        error_text = data.get("errorText", "Unknown error")
                        raise Exception(f"API 호출 실패: {error_text}")

                    if not data.get("data"):
                        print(f"No data found for Market Code: {market_code}, Page: {page_no}")
                        break

                    all_data.extend(data['data'])

                    tot_cnt = data['totCnt']
                    max_page = (tot_cnt // 1000) + 1
                    print(f"Market Code: {market_code}, Page: {page_no}, Total Count: {tot_cnt}, Max Page: {max_page}")  # 디버깅 로그

                    if page_no >= max_page:
                        break
                    page_no += 1
                else:
                    raise Exception(f"API 호출 실패: {response.status_code}")

        # 데이터가 없는 경우 처리
        if not all_data:
            print(f"No data found for date: {date}")
            return {"data": None, "date": date}

        return {"data": all_data, "date": date}

    @task
    def upload_to_s3(file_data):
        """데이터를 CSV로 변환하여 S3에 업로드"""
        data = file_data["data"]
        date = file_data["date"]

        # 데이터가 없는 경우 업로드 건너뛰기
        if not data:
            print(f"No data to upload for date: {date}")
            return

        bucket_name = "team2-s3"
        key = f'raw_data/wholesale/dynamic_wholesale_{date}.csv'

        df = pd.DataFrame(data)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")

        aws_conn = BaseHook.get_connection('aws_conn')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password
        )

        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f"'{key}' -> '{bucket_name}/{key}' 업로드 완료")
        except Exception as e:
            print(f"Error: {e}")

    file_data = collect_data()
    upload_to_s3(file_data)