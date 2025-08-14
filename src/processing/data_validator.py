from pyspark.sql import SparkSession

# MinIO 접속 정보
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = "http://minio:9000"

# Spark 세션 생성 (S3A 설정 포함)
spark = (
    SparkSession.builder.appName("VerifyDataMart")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# MinIO에 저장된 데이터 마트(Delta Lake 테이블) 읽기
try:
    print("Reading data from travel_data_mart...")
    df = spark.read.format("delta").load("s3a://my-bucket/processed/travel-data-mart")
    df.show()
    print("Verification successful!")
except Exception as e:
    print(f"Failed to read data mart. Error: {e}")

spark.stop()
