"""
Kafka Producer 유틸리티
- confluent-kafka-python 라이브러리 사용
- 멱등성(Idempotence)을 보장하여 메시지 중복 전송 방지
"""

import os
import json
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def get_kafka_producer() -> Producer:
    """
    프로젝트 표준 Kafka Producer 인스턴스를 생성하고 반환
    멱등성 옵션 활성화
    """
    try:
        # Producer 설정
        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,  # 멱등성 활성화
        }

        producer = Producer(config)
        logger.info("Confluent Kafka producer with idempotence created successfully")
        return producer

    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise  # 에러 발생 시 프로그램을 중단시키도록 raise 추가
