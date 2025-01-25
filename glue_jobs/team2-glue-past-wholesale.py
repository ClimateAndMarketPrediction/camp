import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# S3 경로
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_OUTPUT_PATH']

# 데이터 로드
df = spark.read.csv(s3_input_path, header=True, encoding='EUC-KR')

# 임시테이블화
df.createOrReplaceTempView("past_wholesale")

# 데이터 정제 처리
result_df = spark.sql("""
SELECT
    AUC_YMD as saleDate,
    WHSL_MRKT_CODE as whsalCd,
    WHSL_MRKT_NM as whsalName,
    LEFT(PDLT_CODE,2) as large,
    "" as largeName,
    substring(PDLT_CODE, length(PDLT_CODE) - 1, 2) as mid,
    PDLT_NM as midName,
    substring(SPCS_CODE, length(SPCS_CODE) - 1, 2)  as small,
    SPCS_NM as smallName,
    MTC_GRAD_CODE as lvCd,
    MTC_GRAD_NM as lvName,
    MTC_NM as sanName,
    SUM(CAST(UNIT_QYT * KG_UNIT_CNVR_QYT AS INT)) AS TOT_QYT,
    SUM(CAST(UNIT_QYT * PRCE AS INT)) AS TOT_PRICE
  FROM past_wholesale
 WHERE MTC_NM IS NOT NULL
   AND WHSL_MRKT_NM IS NOT NULL
GROUP BY AUC_YMD,
        WHSL_MRKT_CODE,
        WHSL_MRKT_NM,
        LEFT(PDLT_CODE,2),
        "",
        substring(PDLT_CODE, length(PDLT_CODE) - 1, 2),
        PDLT_NM,
        substring(SPCS_CODE, length(SPCS_CODE) - 1, 2),
        SPCS_NM,
        MTC_GRAD_CODE,
        MTC_GRAD_NM,
        MTC_NM
ORDER BY AUC_YMD
""")

# 결과 저장
result_df.write.mode("overwrite").format("parquet").option("header", "true").save(s3_output_path)
