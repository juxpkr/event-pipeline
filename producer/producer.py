import json
import time
from kafka import KafkaProducer
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(script_dir, "sample_data.json")


# --- [핵심 수정] 환경 변수를 사용해 카프카 주소 동적으로 설정 ---
# 환경 변수 KAFKA_HOST가 있으면 그 값을 쓰고, 없으면 기본값인 localhost:9092를 사용
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_HOST", "localhost:9092")
print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")


# 카프카 프로듀서 설정
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic_name = "raw-travel-data"

# JSON 파일 읽기
print(f"Opening data file from: {data_file_path}")
with open(data_file_path, "r") as f:
    for line in f:
        data = json.loads(line)
        print(f"Sending data: {data}")
        # 토픽으로 데이터 전송
        producer.send(topic_name, value=data)
        time.sleep(1)  # 1초 간격으로 전송

producer.flush()
print("All data sent.")
