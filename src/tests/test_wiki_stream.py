import json
import time
from requests_sse import EventSource

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def run_live_feed(duration_seconds=10, keywords=None):
    """
    지정된 시간(초) 동안 Wikimedia EventStream을 구독하고,
    수신된 각 이벤트의 핵심 정보를 한 줄씩 출력합니다.

    Args:
        duration_seconds: 실행할 시간(초)
        keywords: 필터링할 키워드 리스트 (None이면 모든 이벤트 출력)
    """
    print(f"--- Starting Live Feed for {duration_seconds} seconds ---")
    
    # 1. 키워드 리스트를 set으로 변환하여 검색 속도를 O(1)으로 최적화합니다.
    keyword_set = set(keywords) if keywords else set()

    if keyword_set:
        print(f"🎯 Filtering for {len(keyword_set)} exact titles in English Wikipedia...")
    else:
        print("📡 Monitoring all events (no filter)")
    print("Press Ctrl+C to stop early.")

    start_time = time.time()
    event_count = 0
    filtered_count = 0

    try:
        headers = {
            'User-Agent': 'GDELT-WikiStream-Monitor/1.0 Python/3.x'
        }
        with EventSource(WIKIMEDIA_STREAM_URL, headers=headers) as event_source:
            for event in event_source:
                if time.time() - start_time > duration_seconds:
                    break

                if event.type == "message":
                    event_count += 1
                    try:
                        change_data = json.loads(event.data)

                        # 2. **영어 위키피디아** 이벤트만 보도록 필터링합니다. (가장 중요!)
                        # 키워드가 영어이므로, 다른 언어 위키는 볼 필요가 없습니다.
                        if change_data.get("wiki") != 'enwiki':
                            continue

                        # 봇 활동은 제외합니다.
                        if change_data.get("bot", False):
                            continue

                        # 3. 키워드 필터링 로직을 **정확하고 단순하게** 변경합니다.
                        # 이벤트의 'title'이 우리가 가진 키워드 set에 정확히 일치하는지 확인합니다.
                        if keyword_set:
                            event_title = change_data.get("title")
                            if event_title not in keyword_set:
                                continue  # 키워드와 일치하지 않으면 이벤트를 건너뜁니다.

                        filtered_count += 1

                        # 핵심 정보 추출
                        timestamp = change_data.get("timestamp")
                        wiki = change_data.get("wiki")
                        event_type = change_data.get("type")
                        user = change_data.get("user")
                        title = change_data.get("title")
                        
                        # 한 줄로 깔끔하게 출력
                        print(
                            f"🎯 [{timestamp}] WIKI:{wiki:<10} | TYPE:{event_type:<5} | USER:{user:<20} | TITLE: {title}"
                        )

                    except json.JSONDecodeError:
                        pass

    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        print("\n--- Live Feed Finished ---")
        print(f"Total events processed: {event_count}")
        if keyword_set:
            print(f"Matching events found: {filtered_count}")
        print(f"Duration: {int(time.time() - start_time)} seconds")


if __name__ == "__main__":
    # GDELT AllNames에서 추출한 키워드 리스트
    target_keywords = [
        "Jim Edgar",
        "Republican Party",
        "Donald Trump",
        "Kamala Harris",
        "Associated Press",
        "Abraham Lincoln Presidential Library Foundation",
    ]
    # 300초(5분) 동안 실행
    run_live_feed(duration_seconds=300, keywords=target_keywords)