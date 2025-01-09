import requests
import re
import csv
import time
import pytz
from datetime import datetime, timedelta
from io import StringIO
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

# 한국 시간대 설정
kst = pytz.timezone('Asia/Seoul')

# 공통 헤더 정의
HEADERS = [
    "TM", "STN", "WS_AVG", "WR_DAY", "WD_MAX", "WS_MAX", "WS_MAX_TM", "WD_INS", "WS_INS",
    "WS_INS_TM", "TA_AVG", "TA_MAX", "TA_MAX_TM", "TA_MIN", "TA_MIN_TM", "TD_AVG", "TS_AVG",
    "TG_MIN", "HM_AVG", "HM_MIN", "HM_MIN_TM", "PV_AVG", "EV_S", "EV_L", "FG_DUR", "PA_AVG",
    "PS_AVG", "PS_MAX", "PS_MAX_TM", "PS_MIN", "PS_MIN_TM", "CA_TOT", "SS_DAY", "SS_DUR",
    "SS_CMB", "SI_DAY", "SI_60M_MAX", "SI_60M_MAX_TM", "RN_DAY", "RN_D99", "RN_DUR",
    "RN_60M_MAX", "RN_60M_MAX_TM", "RN_10M_MAX", "RN_10M_MAX_TM", "RN_POW_MAX", "RN_POW_MAX_TM",
    "SD_NEW", "SD_NEW_TM", "SD_MAX", "SD_MAX_TM", "TE_05", "TE_10", "TE_15", "TE_30", "TE_50"
]

# S3 업로드 함수


def upload_to_s3(csv_data, bucket_name, s3_key):
    aws_conn = BaseHook.get_connection('aws_conn')
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password
    )
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key,
                      Body=csv_data, ContentType='text/csv')
        print(f"S3 업로드 완료: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"S3 업로드 실패: {e}")
        raise Exception(f"S3 업로드 실패: {e}")

# 데이터 다운로드 및 S3 업로드 함수


def download_and_upload_to_s3(dynamic=False, **kwargs):
    bucket_name = "team2-s3"

    if dynamic:  # 동적 데이터 적재
        execution_date = kwargs['execution_date']  # Airflow에서 제공
        data_date = (execution_date).strftime('%Y%m%d')
        print(f"[동적 데이터] 실행일자: {execution_date_kst}, 처리 데이터 날짜: {data_date}")
        date_list = [data_date]
    else:  # 정적 데이터 적재
        YEAR = kwargs.get('YEAR')
        if not YEAR:
            raise ValueError("YEAR가 필요합니다. 정적 데이터 적재에는 YEAR를 전달하세요.")
        start_date = datetime(YEAR, 1, 1)
        end_date = datetime(YEAR, 12, 31)
        date_list = [start_date + timedelta(days=i)
                     for i in range((end_date - start_date).days + 1)]
        date_list = [date.strftime('%Y%m%d') for date in date_list]

    auth_key = Variable.get("climate_api_key")

    for date in date_list:
        time.sleep(2)
        year, month = date[:4], date[4:6]
        # 동적 파일 경로
        s3_key = f"raw_data/climate/{year}/{month}/climate_{date}.csv"
        url = f"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php?tm={date}&stn=0&help=1&authKey={auth_key}"
        response = requests.get(url)
        print(f"{url} 로 데이터 요청 후 응답 대기 중")

        if response.status_code == 200:
            raw_data = response.text
            match = re.search(r"#START7777\n(.*?)\n#7777END",
                              raw_data, re.DOTALL)

            if match:
                extracted_data = match.group(1)
                lines = extracted_data.strip().split("\n")
                data_lines = [line.split(",") for line in lines if not line.startswith(
                    "#") and line.strip()]

                if not data_lines:
                    raise Exception(f"{date} 데이터가 비어 있습니다.")

                # CSV 데이터 작성
                csv_buffer = StringIO()
                writer = csv.writer(csv_buffer)
                writer.writerow(HEADERS)
                writer.writerows(data_lines)
                csv_data = csv_buffer.getvalue()

                # S3 업로드
                upload_to_s3(csv_data, bucket_name, s3_key)
                print(f"{date} 데이터 업로드 완료: {s3_key}")
            else:
                raise Exception(f"{date} 데이터 추출 실패")
        else:
            raise Exception(f"{date} 데이터 요청 실패. 상태 코드: {response.status_code}")


# 정적 데이터 적재 DAG 정의
default_args_static = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),  # 정적 데이터는 2020년부터 실행 가능
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='climate_static_data',
    default_args=default_args_static,
    schedule_interval='@once',
    catchup=False,
    tags=['climate', 's3', 'static'],
) as static_dag:

    static_tasks = []
    previous_task = None
    for year in range(2020, 2025):
        static_task = PythonOperator(
            task_id=f'process_static_{year}_data',
            python_callable=download_and_upload_to_s3,
            op_kwargs={'YEAR': year, 'dynamic': False},
        )
        static_tasks.append(static_task)

        if previous_task:
            previous_task >> static_task  # 태스크 순차 실행
        previous_task = static_task

# 동적 데이터 적재 DAG 정의
default_args_dynamic = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='climate_dynamic_data',
    default_args=default_args_dynamic,
    schedule_interval='30 13 * * *',  # 매일 KST 13:30)
    catchup=False,
    tags=['climate', 's3', 'dynamic'],
) as dynamic_dag:

    dynamic_task = PythonOperator(
        task_id='process_dynamic_data',
        python_callable=download_and_upload_to_s3,
        provide_context=True,  # execution_date 전달
        op_kwargs={'dynamic': True},  # dynamic=True 전달
    )
