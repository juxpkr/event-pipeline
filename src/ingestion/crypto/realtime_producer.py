import websocket
import json
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# --- 설정 ---
# Kafka 접속 정보 (환경 변수 우선)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_HOST", "localhost:9092")
# 구독할 Kafka 토픽 이름
TOPIC_NAME = "realtime-crypto-ticker"
# 구독할 암호화폐 목록
COIN_LIST = [
    "KRW-BTC",
    "KRW-ETH",
    "KRW-XRP",
    "KRW-SOL",
    "KRW-DOGE",
    "KRW-USDT",
    "KRW-STRIKE",
    "KRW-PROVE",
    "KRW-ENA",
    "KRW-ERA",
    "KRW-XLM",
]

# --- Kafka 토픽 생성 ---
print(f"Connecting to Kafka Admin at {KAFKA_BOOTSTRAP_SERVERS}...")
try:
    admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS])
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic], validate_only=False)
    print(f"Topic '{TOPIC_NAME}' created or already exists.")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC_NAME}' already exists.")
except Exception as e:
    print(f"Failed to create Kafka topic. Error: {e}")
finally:
    if "admin_client" in locals():
        admin_client.close()

# --- Kafka 프로듀서 생성 ---
print(f"Initializing Kafka Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# --- 웹소켓 콜백 함수 정의 ---
def on_message(ws, message):
    data = json.loads(message)
    # 수신된 데이터를 Kafka 토픽으로 즉시 전송
    producer.send(TOPIC_NAME, value=data)
    print(f"Sent data to Kafka for coin: {data.get('code', 'N/A')}")


def on_error(ws, error):
    print(f"Error occurred: {error}")


def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Connection Closed ###")


def on_open(ws):
    print("### WebSocket Connection Opened ###")
    subscribe_message = [
        {"ticket": "captin-realtime-stream"},
        {"type": "ticker", "codes": COIN_LIST},
    ]
    ws.send(json.dumps(subscribe_message))


# --- 메인 실행 ---
if __name__ == "__main__":
    ws_url = "wss://api.upbit.com/websocket/v1"
    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    try:
        print("Starting real-time crypto producer...")
        # 웹소켓 연결을 계속 유지하며 데이터 수신
        ws_app.run_forever()
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
        print("Kafka producer closed.")
