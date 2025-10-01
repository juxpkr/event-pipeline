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
    대용량 배치 처리에 최적화된 설정 적용
    """
    try:
        # Producer 설정 - 대용량 데이터 처리 최적화
        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,  # 멱등성 활성화
            # 배치 처리 최적화
            "batch.size": 1048576,  # 1MB (기본값 16KB → 1MB로 증가)
            "linger.ms": 100,  # 100ms 대기로 배치 효율성 증대
            "buffer.memory": 134217728,  # 128MB (기본값 32MB → 128MB로 증가)
            # 처리량 최적화
            "compression.type": "snappy",  # 압축으로 네트워크 부하 감소
            "acks": "all",  # 모든 replica 확인으로 데이터 안전성 보장
            # 대용량 처리 시 안정성
            "retries": 5,  # 재시도 횟수 증가
            "retry.backoff.ms": 1000,  # 재시도 간격 1초
            "max.in.flight.requests.per.connection": 1,  # 순서 보장 (idempotence와 함께)
            # 타임아웃 설정
            "request.timeout.ms": 60000,  # 60초
            "delivery.timeout.ms": 300000,  # 5분
        }

        producer = Producer(config)
        logger.info("High-throughput Kafka producer created successfully")
        logger.info(
            f"Producer config: batch.size={config['batch.size']//1024}KB, buffer.memory={config['buffer.memory']//1024//1024}MB"
        )
        return producer

    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise
