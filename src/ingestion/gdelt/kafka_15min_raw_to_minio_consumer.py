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
    Kafka 'gdelt_raw_events' 토픽에서 15분 GDELT 데이터를 읽어 MinIO 'raw/gdelt_events_15' 버킷에 저장하는 Spark 배치 작업.
    """
    logger.info("🚀 Starting Kafka RAW to MinIO Consumer (Spark Batch Job)...")

    # Spark 세션 생성 (MinIO 접속 정보는 여기서 자동으로 설정됨)
    spark = get_spark_session("KafkaRawToMinIO_Consumer")

    try:
        # Kafka 접속 정보
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        kafka_topic_name = os.getenv("KAFKA_TOPIC_GDELT", "gdelt_raw_events")

        logger.info(f"📥 Reading data from Kafka topic: {kafka_topic_name}")

        # Kafka에서 배치(Batch)로 데이터 읽기
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic_name)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # 데이터가 없으면 종료
        if kafka_df.isEmpty():
            logger.warning("⚠️ No new messages found in Kafka topic. Exiting.")
            return

        # Kafka 메시지의 'value' 컬럼(binary)을 문자열로 변환
        raw_json_df = kafka_df.select(col("value").cast("string").alias("raw_json"))

        # MinIO에 저장할 경로 설정
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        minio_path = f"s3a://raw/gdelt_events_15/{current_time}"

        logger.info(f"💾 Saving data as Parquet to MinIO path: {minio_path}")

        # 데이터를 Parquet 형식으로 MinIO에 저장
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
