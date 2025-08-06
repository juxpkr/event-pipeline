import json
import time
from kafka import KafkaProducer

# Kafka 서버 연결 설정
# docker-compose 내부에서 다른 컨테이너에 접근할 때는 '컨테이너 이름:포트'를 사용한다.
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 보낼 데이터
users = ['user_A', 'user_B', 'user_C', 'user_D']
products = ['product_1', 'product_2', 'product_3', 'product_4', 'product_5']

def generate_log():
    import random
    user_id = random.choice(users)
    product_id = random.choice(products)
    event_type = random.choice(['click', 'view', 'purchase'])

    return {
        'user_id': user_id,
        'event_time': time.time(),
        'event_type': event_type,
        'product_id': product_id
    }

# 'e-commerce-logs'라는 Kafka 토픽에 데이터 전송
topic_name = 'e-commerce-logs'

print(f"[{time.ctime()}] Starting Kafka Producer...")

for i in range(100):
    log_data = generate_log()
    producer.send(topic_name, log_data)
    print(f"[{time.ctime()}] Sent message: {log_data}")
    time.sleep(1)

print(f"[{time.ctime()}] All messages sent.")
producer.close()