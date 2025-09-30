"""
Event Lifecycle Updater
Main lifecycle 테이블 상태 업데이트 담당 클래스
"""

from datetime import datetime, timedelta, timezone
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

# Main lifecycle 테이블 경로 (consolidator가 생성하는 통합 테이블)
MAIN_LIFECYCLE_PATH = "s3a://warehouse/audit/lifecycle"


class EventLifecycleUpdater:
    """Main lifecycle 테이블 상태 업데이트 관리자"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.lifecycle_path = MAIN_LIFECYCLE_PATH

    def mark_silver_processing_complete(self, events_df: DataFrame, batch_id: str):
        """Silver 처리 완료 상태로 업데이트 (DataFrame 기반)"""
        if events_df is None or events_df.rdd.isEmpty():
            return 0

        current_time = datetime.now(timezone.utc)

        # DataFrame에서 global_event_id만 추출 (collect 없이)
        event_ids_df = events_df.select("global_event_id").distinct()
        event_ids_df.createOrReplaceTempView("silver_completed_events_temp")

        merge_sql = f"""
        MERGE INTO delta.`{self.lifecycle_path}` AS lifecycle
        USING silver_completed_events_temp AS events
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type IN ('EVENT', 'GKG')
        WHEN MATCHED THEN
        UPDATE SET
            audit.silver_processing_end_time = '{current_time}',
            status = 'SILVER_COMPLETE'
        """

        self.spark.sql(merge_sql)

        # 업데이트된 레코드 수 반환
        updated_count = event_ids_df.count()
        return updated_count

    def mark_gold_processing_complete(self, event_ids: list, batch_id: str):
        """Gold 처리 완료 상태로 업데이트"""
        if not event_ids:
            return 0

        current_time = datetime.now(timezone.utc)

        event_df = self.spark.createDataFrame(
            [(event_id,) for event_id in event_ids], ["global_event_id"]
        )
        event_df.createOrReplaceTempView("gold_complete_events_temp")

        merge_sql = f"""
        MERGE INTO delta.`{self.lifecycle_path}` AS lifecycle
        USING gold_complete_events_temp AS events
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type IN ('EVENT', 'GKG')
        WHEN MATCHED THEN
        UPDATE SET
            audit.gold_processing_end_time = '{current_time}',
            status = 'GOLD_COMPLETE'
        """

        self.spark.sql(merge_sql)
        return len(event_ids)

    def mark_postgres_migration_complete(self, event_ids: list, batch_id: str):
        """Postgres 마이그레이션 완료 상태로 업데이트"""
        if not event_ids:
            return 0

        current_time = datetime.now(timezone.utc)

        event_df = self.spark.createDataFrame(
            [(event_id,) for event_id in event_ids], ["global_event_id"]
        )
        event_df.createOrReplaceTempView("postgres_complete_events_temp")

        merge_sql = f"""
        MERGE INTO delta.`{self.lifecycle_path}` AS lifecycle
        USING postgres_complete_events_temp AS events
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type IN ('EVENT', 'GKG')
        WHEN MATCHED THEN
        UPDATE SET
            audit.postgres_migration_end_time = '{current_time}',
            status = 'POSTGRES_COMPLETE'
        """

        self.spark.sql(merge_sql)
        return len(event_ids)

    def bulk_update_status(self, from_status: str, to_status: str):
        """특정 상태의 모든 EVENT/GKG 타입 이벤트를 다른 상태로 업데이트"""
        current_time = datetime.now(timezone.utc)

        update_sql = f"""
        UPDATE delta.`{self.lifecycle_path}`
        SET status = '{to_status}',
            audit.postgres_migration_end_time = '{current_time}'
        WHERE status = '{from_status}' AND event_type IN ('EVENT', 'GKG')
        """

        self.spark.sql(update_sql)

        # 업데이트된 레코드 수 반환
        count_sql = f"""
        SELECT COUNT(*) as count FROM delta.`{self.lifecycle_path}`
        WHERE status = '{to_status}' AND event_type IN ('EVENT', 'GKG')
        """
        result = self.spark.sql(count_sql).collect()[0]
        return result["count"]

    def expire_old_waiting_events(self, hours_threshold: int = 24):
        """24시간 이상 대기 중인 이벤트들을 EXPIRED로 변경"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

        # SQL UPDATE로 만료 처리
        update_sql = f"""
        UPDATE delta.`{self.lifecycle_path}`
        SET status = 'EXPIRED'
        WHERE status = 'WAITING'
        AND event_type IN ('EVENT', 'GKG')
        AND audit.bronze_arrival_time < '{cutoff_time}'
        """

        # 업데이트 전 카운트 확인
        count_sql = f"""
        SELECT COUNT(*) as expired_count
        FROM delta.`{self.lifecycle_path}`
        WHERE status = 'WAITING'
        AND event_type IN ('EVENT', 'GKG')
        AND audit.bronze_arrival_time < '{cutoff_time}'
        """

        expired_count = self.spark.sql(count_sql).collect()[0]["expired_count"]

        # 실제 업데이트 실행
        self.spark.sql(update_sql)

        print(f"Expired {expired_count} events older than {hours_threshold} hours")
        return expired_count

    def get_lifecycle_stats(self, hours_back: int = 24) -> dict:
        """최근 N시간 lifecycle 통계 조회"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)

        lifecycle_df = self.spark.read.format("delta").load(self.lifecycle_path)
        recent_events = lifecycle_df.filter(
            col("audit.bronze_arrival_time") >= lit(cutoff_time)
        )

        stats = recent_events.groupBy("status").count().collect()

        result = {
            "total_events": recent_events.count(),
            "waiting_events": 0,
            "joined_events": 0,
            "expired_events": 0,
            "join_success_rate": 0.0,
        }

        for row in stats:
            if row["status"] == "WAITING":
                result["waiting_events"] = row["count"]
            elif row["status"] == "JOINED":
                result["joined_events"] = row["count"]
            elif row["status"] == "EXPIRED":
                result["expired_events"] = row["count"]

        # 조인 성공률 계산
        completed_events = result["joined_events"] + result["expired_events"]
        if completed_events > 0:
            result["join_success_rate"] = (
                result["joined_events"] / completed_events
            ) * 100.0

        return result
