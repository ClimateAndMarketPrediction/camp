import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.functions import to_date, col, when
from pyspark.sql.functions import date_format
import logging

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
df = spark.read.csv(args['S3_INPUT_PATH'], header=True, schema=schema)

# 데이터 저장
logger.info(f"Saving transformed data to S3 path: {args['S3_OUTPUT_PATH']}")
df.write.mode("overwrite").format("parquet").option("header", "true").save(args['S3_OUTPUT_PATH'])

logger.info("### Glue ETL Job Completed Successfully ###")