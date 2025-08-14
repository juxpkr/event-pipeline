import json
import time
from requests_sse import EventSource

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def run_live_feed(duration_seconds=10):
    """
    지정된 시간(초) 동안 Wikimedia EventStream을 구독하고,
    수신된 각 이벤트의 핵심 정보를 한 줄씩 출력합니다.
    """
    print(f"--- Starting Live Feed for {duration_seconds} seconds ---")
    print("Press Ctrl+C to stop early.")

    start_time = time.time()
    event_count = 0

    try:
        # EventSource에 연결
        with EventSource(WIKIMEDIA_STREAM_URL) as event_source:
            for event in event_source:
                # 지정된 시간이 지나면 종료
                if time.time() - start_time > duration_seconds:
                    break

                if event.type == "message":
                    event_count += 1
                    try:
                        # 들어온 JSON 데이터를 파싱
                        change_data = json.loads(event.data)

                        # 우리가 보고 싶은 핵심 정보만 추출
                        timestamp = change_data.get("timestamp")
                        wiki = change_data.get("wiki")
                        event_type = change_data.get("type")
                        title = change_data.get("title")
                        user = change_data.get("user")
                        is_bot = change_data.get("bot", False)

                        # 한 줄로 깔끔하게 출력!
                        print(
                            f"[{timestamp}] WIKI:{wiki:<10} | TYPE:{event_type:<5} | BOT:{str(is_bot):<5} | USER:{user:<20} | TITLE: {title}"
                        )

                    except json.JSONDecodeError:
                        # 가끔 오는 비정상 데이터는 무시
                        pass

    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        print(
            f"\n--- Live Feed Finished ({event_count} events in {duration_seconds} seconds) ---"
        )


if __name__ == "__main__":
    # 10초 동안만 테스트 실행 (너무 길면 화면이 미친듯이 올라감)
    run_live_feed(duration_seconds=10)
