import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 데이터 로드 (스키마 정의 없이)
df = spark.read.csv(args['S3_INPUT_PATH'], header=True, inferSchema=True)

# 데이터 변환 (SQL 쿼리에서 타입 변환)
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

# 결과 저장 (Parquet 형식)
result_df.write.mode("overwrite").format("parquet").option("header", "true").save(args['S3_OUTPUT_PATH'])

print("### Glue ETL Job Completed Successfully ###")
