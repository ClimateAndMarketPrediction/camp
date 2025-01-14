import sys
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
import logging

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH', 'DATA_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 경로 및 데이터 날짜
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_OUTPUT_PATH']
data_date = args['DATA_DATE']

logger.info(f"file date: {data_date}")

data_month = data_date[5:7]

# 파일 경로 생성
file_path = f"{s3_input_path}/{data_month}/retail_{data_date}.csv"

# 스키마 정의
schema = StructType([
    StructField("saleDate", StringType(), True),
    StructField("large", StringType(), True),
    StructField("mid", StringType(), True),
    StructField("midName", StringType(), True),
    StructField("small", StringType(), True),
    StructField("smallName", StringType(), True),
    StructField("lv", StringType(), True),
    StructField("lvcd", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("avgAnnualAmt", IntegerType(), True),
])

# 데이터 로드
logger.info(f"Loading data from S3 path: {args['S3_INPUT_PATH']}")
df = spark.read.csv(file_path, header=True, schema=schema)

# 데이터 저장
logger.info(f"Saving transformed data to S3 path: {s3_output_path}")
df.write.mode("overwrite").format("parquet").option("header", "true").save(s3_output_path)

print("### ETL Job Completed for Dynamic Data with CAST ###")
