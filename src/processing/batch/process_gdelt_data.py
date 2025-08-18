from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# TODO: 모든 설정 값(Kafka 주소, MinIO 주소 등)을 config 파일에서 읽어오도록 수정 필요


def get_spark_session():
    # Spark 세션을 생성하고 반환한다.
    # MinIO, Delta Lake를 연결해야 한다.

    print("Initializing Spark session for MinIO/Delta Lake...")
    spark = (
        SparkSession.builder.appName("GDELT_Silver_Processor")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    print("Spark session initialized successfully.")
    return spark


def define_schema():
    # Kafka에서 들어오는 GDELT JSON 데이터의 스키마를 정의한다.

    return StructType(
        [
            StructField("event_date", StringType(), True),
            StructField("Actor1CountryCode", StringType(), True),
            StructField("EventRootCode", StringType(), True),
            StructField("day_of_week", LongType(), True),
            StructField("event_count", LongType(), True),
            StructField("avg_conflict_score", DoubleType(), True),
            StructField("stddev_conflict_score", DoubleType(), True),
            StructField("avg_tone", DoubleType(), True),
            StructField("unique_source_count", LongType(), True),
        ]
    )


def process_kafka_to_delta(spark: SparkSession, schema: StructType):
    # Kafka 스트림을 읽어서 Delta Lake(MinIO)에 Silver 테이블로 저장한다.

    kafka_topic = "gdelt_events"
    minio_path = "s3a://silver/gdelt_events"
    checkpoint_path = "s3a://checkpoints/gdelt_events"

    print(f"Reading stream from Kafka topic: {kafka_topic}...")

    # Kafka 소스에서 데이터 스트림으로 읽기
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # JSON 문자열을 우리가 정의한 스키마에 맞춰 파싱
    processed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    print(f"Writing stream to Delta Lake path: {minio_path}...")

    # 처리된 데이터프레임을 Delta Lake 형식으로 MinIO에 스트리밍 쓰기
    query = (
        processed_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(minio_path)
    )

    query.awaitTermination()


def main():
    # 메인 실행 함수
    spark = get_spark_session()
    schema = define_schema()
    process_kafka_to_delta(spark, schema)


if __name__ == "__main__":
    main()
