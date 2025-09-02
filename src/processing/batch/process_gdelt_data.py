import os
import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 프로젝트 루트를 Python path에 추가 (Docker 환경변수 우선, 없으면 자동 계산)
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, project_root)

from src.utils.spark_builder import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def define_schema() -> StructType:
    # Kafka에서 들어오는 GDELT JSON 데이터의 스키마를 정의한다.
    return StructType(
        [
            StructField("GlobalEventID", StringType(), True),
            StructField("Day", StringType(), True),
            StructField("MonthYear", StringType(), True),
            StructField("Year", StringType(), True),
            StructField("FractionDate", StringType(), True),
            StructField("Actor1Code", StringType(), True),
            StructField("Actor1Name", StringType(), True),
            StructField("Actor1CountryCode", StringType(), True),
            StructField("Actor1KnownGroupCode", StringType(), True),
            StructField("Actor1EthnicCode", StringType(), True),
            StructField("AvgTone", StringType(), True),
            StructField("EventRootCode", StringType(), True),
            StructField("QuadClass", StringType(), True),
        ]
    )


def process_kafka_to_delta(spark: SparkSession, schema: StructType):
    # Kafka 스트림을 읽어서 Delta Lake(MinIO)에 Silver 테이블로 저장한다.

    kafka_topic = "gdelt_events"
    minio_path = "s3a://warehouse/silver/gdelt_events"
    checkpoint_path = "s3a://warehouse/checkpoints/gdelt_events_silver"
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

    logger.info(f"Reading stream from Kafka topic: {kafka_topic}...")

    # Kafka 소스에서 데이터 스트림으로 읽기
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # JSON 문자열을 우리가 정의한 스키마에 맞춰 파싱
    processed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    logger.info(f"Writing stream to Delta Lake path: {minio_path}...")

    # 처리된 데이터프레임을 Delta Lake 형식으로 MinIO에 스트리밍 쓰기
    query = (
        processed_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")  # 스키마 자동 병합
        .trigger(availableNow=True)
        .start(minio_path)
    )

    query.awaitTermination()


def main():
    # 메인 실행 함수
    logger.info("Starting GDELT Silver Processor Spark Job")

    # Spark_builder를 사용
    # Spark 설정은 모두 src/utils/spark_builder.py에서 관리
    spark = get_spark_session("GDLET_Silver_Processor")
    schema = define_schema()

    try:
        process_kafka_to_delta(spark, schema)
    except Exception as e:
        logger.error("An unexpected error occurred in the Spark job.", exc_info=True)
    finally:
        logger.info("Spark job finished. Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
