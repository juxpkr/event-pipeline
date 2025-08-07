from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# MinIO 접속 정보 설정
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = "http://minio:9000"

# Spark 세션 생성 (S3A 설정 추가)
spark = (
    SparkSession.builder.appName("KafkaToDelta")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  # MinIO 사용 시 필수!
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # --- [핵심 수정] Delta Lake를 위한 필수 설정 추가! ---
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# 스키마 정의 (JSON 데이터 구조)
schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("trend_score", IntegerType(), True),
    ]
)

# Kafka 소스에서 스트리밍 데이터 읽기
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw-travel-data")
    .load()
)

# JSON 문자열을 실제 데이터 컬럼으로 변환
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

# trend_score가 90 이상인 데이터만 필터링
popular_travel_df = json_df.filter(col("trend_score") >= 90)

# 1. Delta Lake 테이블로 저장 (json_df 대신 popular_travel_df를 저장)
delta_query = (
    popular_travel_df.writeStream.outputMode("append")
    .format("delta")
    .option(
        "checkpointLocation", "s3a://my-bucket/checkpoints/travel-data-mart_delta"
    )  # 체크포인트 경로도 바꿔주자
    .start("s3a://my-bucket/processed/travel-data-mart")  # 저장 경로도 새로 지정
)


# 2. PostgreSQL로 스트리밍 데이터를 보내는 로직 추가
def write_to_postgres(df, epoch_id):
    # PostgreSQL 접속 정보 (Airflow와 같은 DB 사용)
    postgres_url = "jdbc:postgresql://postgres:5432/airflow"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }
    table_name = "travel_data_mart"

    # 데이터를 PostgreSQL에 쓴다 (매번 전체 데이터를 덮어쓰기)
    df.write.jdbc(
        url=postgres_url,
        table=table_name,
        mode="overwrite",
        properties=postgres_properties,
    )
    print(f"Batch {epoch_id} written to PostgreSQL.")

    # popular_travel_df를 PostgreSQL에 foreachBatch를 사용해 저장
    postgres_query = (
        popular_travel_df.writeStream.foreachBatch(write_to_postgres)
        .outputMode("update")  # Overwrite 모드와 함께 쓰기 위해 update 모드 사용
        .option(
            "checkpointLocation",
            "s3a://my-bucket/checkpoints/travel_data_mart_postgres",
        )
        .start()
    )


# 모든 스트리밍 쿼리가 끝날 때까지 기다림
spark.streams.awaitAnyTermination()
