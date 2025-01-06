from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import time

# S3 설정
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_FILE_PREFIX = Variable.get("RETAIL_S3_PREFIX")

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

# 데이터 가져오기 함수
def fetch_data_for_date(date, api_key, cert_id, category_code):
    params = {
        "p_cert_key": api_key,
        "p_cert_id": cert_id,
        "p_regday": date,
        "p_item_category_code": category_code,
        **DEFAULT_PARAMS
    }

    temp_category_name = {"200":"채소류", "400":"과일류", "500":"축산물", "600":"수산물"}
    print(f"{date}일자 {temp_category_name[category_code]} 데이터 수집 시작")

    retries = 3  # 재시도 횟수
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            items = []
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
                    'avgAnnualAmt': 0 if item.findtext('dpr7') == "-" else int(item.findtext('dpr7', '0').replace(',', ''))
                }
                if data['mid']:
                    items.append(data)
            return items
        except requests.exceptions.RequestException as e:
            print(f"{attempt + 1}번째 시도 실패 - 날짜: {date}, 카테고리: {category_code}, 오류: {e}")
            time.sleep(2)  # 재시도 전 대기 시간
    return None

# 데이터 가져오기 작업
def fetch_data_task(**kwargs):
    # execution_date에 하루를 더하여 데이터 수집 날짜로 사용
    target_date = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y-%m-%d")

    all_data = []
    api_index = 0
    for category_code in CATEGORY_CODES:
        current_api = API_KEYS[api_index]
        result = fetch_data_for_date(
            target_date, current_api["API_KEY"], current_api["CERT_ID"], category_code
        )
        if result:
            all_data.extend(result)
        else:
            print(f"API KEY 오류로 인한 키 전환. 일시: {target_date}, 카테고리: {category_code}")
            api_index = (api_index + 1) % len(API_KEYS)
            current_api = API_KEYS[api_index]
            result = fetch_data_for_date(
                target_date, current_api["API_KEY"], current_api["CERT_ID"], category_code
            )
            if result:
                all_data.extend(result)

    if all_data:
        # 데이터를 DataFrame으로 변환 후 반환
        df = pd.DataFrame(all_data)
        print(f"{target_date} 데이터를 성공적으로 수집했습니다.")
        return df.to_dict('records')  # DataFrame을 리스트로 변환하여 반환
    else:
        raise ValueError(f"{target_date}에 데이터를 가져오지 못했습니다.")



def upload_data_task(**kwargs):
    # execution_date에 하루를 더하여 저장 날짜로 사용
    target_date = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y-%m-%d")
    all_data = kwargs['ti'].xcom_pull(task_ids='fetch_data_task')

    if not all_data:
        raise ValueError("업로드할 데이터가 없습니다.")

    # 실행 날짜에서 연도와 월 추출
    year = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y")
    month = (kwargs['execution_date'] + timedelta(days=1)).strftime("%m")

    # 파일 이름을 설정
    file_name = f"retail_{target_date}.csv"
    s3_key = f"{S3_FILE_PREFIX}{year}/{month}/{file_name}"

    # DataFrame 생성
    df = pd.DataFrame(all_data)

    # 데이터를 메모리에 CSV로 저장
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # S3에 업로드
    s3 = S3Hook(aws_conn_id='aws_conn')
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    print(f"S3에 데이터 업로드 완료: s3://{S3_BUCKET_NAME}/{s3_key}")


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
    'retail_dynamic_dag_s3',
    default_args=default_args,
    description='일별 KAMIS 소매 데이터 수집 및 S3 업로드',
    schedule_interval='30 13 * * *',
    start_date = datetime(2024, 12, 31),
    catchup=True, 
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