"""
Event Lifecycle Expiration Script
오래된 WAITING 상태 이벤트들을 EXPIRED로 변경
"""

import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from src.utils.spark_builder import create_spark_session
from src.audit.lifecycle_tracker import EventLifecycleTracker


def main():
    """오래된 lifecycle 이벤트들 만료 처리"""
    spark = create_spark_session("expire_lifecycle_events")

    try:
        tracker = EventLifecycleTracker(spark)

        # 48시간 이상 대기 중인 이벤트들을 만료 처리
        expired_count = tracker.expire_old_waiting_events(hours_threshold=48)

        print(f"Successfully expired {expired_count} old events")

    except Exception as e:
        print(f"Error during lifecycle expiration: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()