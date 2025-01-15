from airflow import DAG
from airflow.models import XCom
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import provide_session

# XCom 데이터를 삭제하는 함수


@provide_session
def delete_all_xcom(session=None):
    """
    모든 XCom 데이터를 삭제합니다.
    """
    try:
        session.query(XCom).delete()
        session.commit()
        print("All XCom data has been deleted successfully.")
    except Exception as e:
        session.rollback()
        print(f"Failed to delete XCom data: {e}")
        raise


# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # 어제 날짜부터 시작
    'retries': 0,  # 재시도 없음
}

dag = DAG(
    'clear_all_xcom',
    default_args=default_args,
    schedule_interval='@once',  # 한 번만 실행
    catchup=False,  # 과거 작업은 실행하지 않음
    tags=['cleanup'],
)

# XCom 데이터 삭제 작업
delete_xcom_task = PythonOperator(
    task_id='delete_all_xcom',
    python_callable=delete_all_xcom,
    dag=dag,
)

# DAG 실행 순서
delete_xcom_task
