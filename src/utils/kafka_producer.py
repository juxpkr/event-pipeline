import os
import json
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def get_kafka_producer() -> KafkaProducer:
    # 프로젝트 표준 KafkaProducer 인스턴스를 생성하고 반환한다.
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            batch_size=16384,
            linger_ms=100,
            compression_type="gzip",
        )
        logger.info("✅ Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        raise  # 에러 발생 시 프로그램을 중단시키도록 raise 추가
