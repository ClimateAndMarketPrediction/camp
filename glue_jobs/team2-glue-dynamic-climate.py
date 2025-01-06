import sys
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH', 'DATA_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 경로 및 데이터 날짜
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_OUTPUT_PATH']
data_date = args['DATA_DATE']

formatted_date = data_date.replace("-", "")  # 2022-02-02 -> 20220202
data_month = formatted_date[4:6]

# 파일 경로 생성
yesterday_file_path = f"{s3_input_path}/{data_month}/climate_{formatted_date}.csv"
print(f"Processing file: {yesterday_file_path}")

# 데이터 로드 (스키마 자동 추론)
df = spark.read.csv(yesterday_file_path, header=True, inferSchema=True)

# 데이터 변환 (SQL 쿼리에서 CAST 사용)
df.createOrReplaceTempView("weather_data")
result_df = spark.sql("""
SELECT 
    CAST(TM AS STRING) AS observation_date,        
    CAST(STN AS INT) AS station_id,            
    CAST(TA_AVG AS FLOAT) AS avg_temperature,    
    CAST(TA_MAX AS FLOAT) AS max_temperature,    
    CAST(TA_MIN AS FLOAT) AS min_temperature,    
    CAST(RN_DAY AS FLOAT) AS daily_rainfall,
    CAST(SD_NEW AS FLOAT) AS new_snowfall        
FROM weather_data
""")

# 변환된 데이터를 S3에 저장 (중복 방지, 새로운 데이터만 저장)
result_df.write.mode("overwrite").format("parquet").save(s3_output_path)

print("### ETL Job Completed for Dynamic Data with CAST ###")
