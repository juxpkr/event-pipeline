import websocket
import json
import time
import threading
import os

# 글로벌 변수: 데이터 저장용
collected_data = []
data_lock = threading.Lock()


# 웹소켓 서버에 메시지가 도착했을 떄 실행될 함수
def on_message(ws, message):
    # 수신된 데이터를 파이썬 딕셔너리로 변환
    data = json.loads(message)

    # 데이터를 리스트에 안전하게 추가
    with data_lock:
        collected_data.append(data)

    # 간단한 진행상황만 표시 (매 10개마다)
    if len(collected_data) % 10 == 0:
        print(f"수집된 데이터: {len(collected_data)}개")


# 에러 발생 시 실행될 함수
def on_error(ws, error):
    print(f"Error : {error}")


# 웹소켓 연결이 닫혔을 때 실행될 함수
def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")


# 웹소켓 연결이 성공적으로 열렸을 때 실행될 함수
def on_open(ws):
    print("### Connection Opened")
    # 구독할 내용을 서버에 전송
    subcribe_message = [
        {"ticket": "test"},
        {
            "type": "ticker",
            "codes": [
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
            ],
        },
    ]
    ws.send(json.dumps(subcribe_message))


def save_data_to_file():
    """3분 후 수집된 데이터를 JSONL 파일로 저장"""
    # 현재 스크립트의 상위 디렉토리 (프로젝트 루트)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data")
    file_path = os.path.join(data_dir, "crypto_stream_data_sample.jsonl")

    # data 디렉토리가 없으면 생성
    os.makedirs(data_dir, exist_ok=True)

    with data_lock:
        print(f"\n=== 3분 완료! 총 {len(collected_data)}개의 데이터를 저장합니다 ===")

        with open(file_path, "w", encoding="utf-8") as f:
            for data_item in collected_data:
                f.write(json.dumps(data_item, ensure_ascii=False) + "\n")

        print(f"데이터 저장 완료: {file_path}")
        print(f"저장된 레코드 수: {len(collected_data)}")


if __name__ == "__main__":
    ws_url = "wss://api.upbit.com/websocket/v1"

    # 웹소켓 앱 생성
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    print("=== 암호화폐 실시간 데이터 수집 시작 (3분간) ===")

    # 3분 후에 웹소켓 종료하고 데이터 저장하는 타이머 설정
    def stop_and_save():
        save_data_to_file()
        ws.close()

    timer = threading.Timer(180.0, stop_and_save)  # 180초 = 3분
    timer.start()

    try:
        # 프로그램이 종료되지 않고 계속 서버로부터 데이터를 받도록 실행
        ws.run_forever()
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단됨")
        timer.cancel()
        save_data_to_file()
    finally:
        print("웹소켓 연결 종료")
