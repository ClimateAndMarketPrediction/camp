from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dateutil.relativedelta import relativedelta
import pandas as pd
import time

# S3 설정
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_FILE_PREFIX_STATIC = Variable.get("RETAIL_S3_PREFIX_STATIC")

# API 설정
API_KEYS = Variable.get("RETAIL_API_KEY", deserialize_json=True)
BASE_URL = "http://www.kamis.or.kr/service/price/xml.do?action=dailyPriceByCategoryList"
CATEGORY_CODES = ["200", "400", "500", "600"]
DEFAULT_PARAMS = {
    "p_product_cls_code": "01",
    "p_country_code": "",
    "p_returntype": "xml",
    "p_convert_kg_yn": "N",
}

# 월간 데이터 수집 함수
def fetch_monthly_data(start_date, end_date, api_key, cert_id, category_code):
    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    temp_category_name = {"200": "채소류", "400": "과일류", "500": "축산물", "600": "수산물"}
    all_items = []

    for date in date_range:
        params = {
            "p_cert_key": api_key,
            "p_cert_id": cert_id,
            "p_regday": date,
            "p_item_category_code": category_code,
            **DEFAULT_PARAMS,
        }
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            for item in root.findall('.//item'):
                data = {
                    'saleDate': date,
                    'large': temp_category_name[category_code],
                    'mid': item.findtext('item_code'),
                    'midName': item.findtext('item_name'),
                    'small': item.findtext('item_code'),
                    'smallName': item.findtext('kind_name').split('(')[0].strip() if item.findtext('kind_name') else None,
                    'lv': item.findtext('rank'),
                    'lvCd': item.findtext('rank_code'),
                    'unit': item.findtext('unit'),
                    'amount': 0 if item.findtext('dpr1') == "-" else int(item.findtext('dpr1', '0').replace(',', '')),
                    'avgAnnualAmt': 0 if item.findtext('dpr7') == "-" else int(item.findtext('dpr7', '0').replace(',', '')),
                }
                all_items.append(data)
        except requests.exceptions.RequestException as e:
            print(f"날짜: {date}, 카테고리: {category_code}, 오류: {e}")
            time.sleep(2)
    return all_items


# Airflow 태스크 함수 - 데이터 수집
def fetch_data_task(**kwargs):
    execution_date = kwargs['execution_date'] + relativedelta(months=1)  # 1달 추가
    start_date = execution_date.replace(day=1)  # 해당 월의 첫날
    end_date = (execution_date + relativedelta(months=1)).replace(day=1) - timedelta(days=1)  # 해당 월의 마지막 날

    print(f"데이터 수집 범위: {start_date} ~ {end_date}")

    all_data = []
    for category_code in CATEGORY_CODES:
        api_key_data = API_KEYS[0]  # 첫 번째 API 키 사용
        result = fetch_monthly_data(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            api_key_data["API_KEY"],
            api_key_data["CERT_ID"],
            category_code
        )
        all_data.extend(result)

    if not all_data:
        raise ValueError(f"{start_date.strftime('%Y-%m')}에 데이터를 가져오지 못했습니다.")
    
    return all_data


# Airflow 태스크 함수 - 데이터 업로드
def upload_data_task(**kwargs):
    execution_date = kwargs['execution_date'] + relativedelta(months=1)  # 1달 추가
    all_data = kwargs['ti'].xcom_pull(task_ids='fetch_data_task')

    if not all_data:
        print("업로드할 데이터가 없습니다.")
        return

    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    file_name = f"retail_{year}-{month}.csv"
    s3_key = f"{S3_FILE_PREFIX_STATIC}{year}/{file_name}"  # 년도별 하위 폴더에 저장

    df = pd.DataFrame(all_data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = S3Hook(aws_conn_id='aws_conn')
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    print(f"S3에 업로드 완료: s3://{S3_BUCKET_NAME}/{s3_key}")


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'retail_static_dag_s3',
    default_args=default_args,
    description='KAMIS 소매 데이터 수집 및 S3 업로드',
    schedule_interval='@monthly',  # 매월 실행
    start_date=datetime(2020, 1, 1),  # 2020년 1월 1일 시작
    end_date=datetime(2024, 12, 1),
    max_active_runs=3,  # 동시에 실행되는 DAG 인스턴스를 3개로 제한
    concurrency=2,      # 동시에 실행되는 태스크를 2개로 제한
    catchup=True,  # 과거 데이터 처리 활성화
)

fetch_data = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data_task,
    dag=dag,
)

upload_data = PythonOperator(
    task_id='upload_data_task',
    python_callable=upload_data_task,
    dag=dag,
)

fetch_data >> upload_data