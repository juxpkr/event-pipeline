import os
import sys
from pathlib import Path
import logging
from pyspark.sql.functions import col
import datetime

# 프로젝트 루트를 Python path에 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, project_root)

from src.utils.spark_builder import get_spark_session

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    """
    Kafka 'gdelt_events' 토픽에서 데이터를 읽어 MinIO 'raw' 버킷에 저장하는 Spark 배치 작업.
    프로젝트 표준 방식인 Spark를 사용하여 MinIO에 연결합니다.
    """
    logger.info("🚀 Starting Kafka to MinIO Consumer (Spark Batch Job)...")

    # Spark 세션 생성 (MinIO 접속 정보는 여기서 자동으로 설정됨)
    spark = get_spark_session("KafkaToMinIO_Consumer")

    try:
        # Kafka 접속 정보
        kafka_bootstrap_servers = "kafka:29092"
        kafka_topic_name = "gdelt_events"

        logger.info(f"📥 Reading data from Kafka topic: {kafka_topic_name}")

        # Kafka에서 배치(Batch)로 데이터 읽기
        # startingOffsets/endingOffsets를 사용해 해당 시점에 처리 가능한 모든 데이터를 읽음
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic_name)
            .option("startingOffsets", "earliest")
            .option(
                "endingOffsets", "latest"
            )  # 배치 작업이므로 최신 데이터까지 읽고 종료
            .load()
        )

        # 데이터가 없으면 종료
        if kafka_df.isEmpty():
            logger.warning("⚠️ No new messages found in Kafka topic. Exiting.")
            return

        # Kafka 메시지의 'value' 컬럼(binary)을 문자열로 변환
        # JSON의 구조를 파싱하지 않고, 원본 그대로 저장
        raw_json_df = kafka_df.select(col("value").cast("string").alias("raw_json"))

        # MinIO에 저장할 경로 설정
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # "historical"을 경로에 추가하고 Parquet 형식으로 저장
        minio_path = f"s3a://raw/gdelt_events_historical/{current_time}"

        logger.info(f"💾 Saving data as Parquet to MinIO path: {minio_path}")

        # 데이터를 Parquet 형식으로 MinIO에 저장
        # Spark는 기본적으로 Snappy 압축을 사용하므로 .snappy.parquet 파일이 생성됩니다.
        (raw_json_df.write.format("parquet").mode("overwrite").save(minio_path))

        record_count = raw_json_df.count()
        logger.info(f"🎉 Successfully saved {record_count} records to {minio_path}")

    except Exception as e:
        logger.error(f"❌ An error occurred during the Spark job: {e}", exc_info=True)
    finally:
        logger.info("✅ Spark session closed.")
        spark.stop()


if __name__ == "__main__":
    main()
