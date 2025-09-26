"""
Event Lifecycle Tracker
이벤트 생명주기 추적을 위한 Delta Lake 관리 모듈
"""

import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

LIFECYCLE_PATH = "s3a://warehouse/audit/event_lifecycle"


class EventLifecycleTracker:
    """이벤트 생명주기 추적 관리자"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.lifecycle_path = LIFECYCLE_PATH

    def initialize_table(self):
        """lifecycle 테이블 초기 생성 (1회만)"""
        schema = StructType(
            [
                StructField("global_event_id", StringType(), False),
                StructField("event_type", StringType(), False),  # EVENT, GKG
                StructField("audit", StructType([
                    StructField("bronze_arrival_time", TimestampType(), False),
                    StructField("silver_processing_end_time", TimestampType(), True),
                    StructField("gold_processing_end_time", TimestampType(), True),
                    StructField("postgres_migration_end_time", TimestampType(), True)
                ]), False),
                StructField("status", StringType(), False),  # WAITING, SILVER_COMPLETE, GOLD_COMPLETE, POSTGRES_COMPLETE
                StructField("batch_id", StringType(), False),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
                StructField("hour", IntegerType(), False),
            ]
        )

        # 더미 데이터로 테이블 구조 생성
        dummy_data = self.spark.createDataFrame([], schema)
        dummy_data.write.format("delta").mode("overwrite").partitionBy(
            "year", "month", "day", "hour"
        ).save(self.lifecycle_path)

        print(f"Event lifecycle table initialized at {self.lifecycle_path}")

    def track_bronze_arrival(self, events_df: DataFrame, batch_id: str, event_type: str):
        """Bronze 도착 이벤트들을 lifecycle에 기록"""
        current_time = datetime.now(timezone.utc)

        # audit 구조체 생성
        from pyspark.sql.functions import struct

        lifecycle_records = (
            events_df.select("global_event_id")
            .distinct()
            .withColumn("event_type", lit(event_type))  # EVENT 또는 GKG
            .withColumn("audit", struct(
                lit(current_time).alias("bronze_arrival_time"),
                lit(None).cast(TimestampType()).alias("silver_processing_end_time"),
                lit(None).cast(TimestampType()).alias("gold_processing_end_time"),
                lit(None).cast(TimestampType()).alias("postgres_migration_end_time")
            ))
            .withColumn("status", lit("WAITING"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("year", year(lit(current_time)))
            .withColumn("month", month(lit(current_time)))
            .withColumn("day", dayofmonth(lit(current_time)))
            .withColumn("hour", hour(lit(current_time)))
        )

        # Delta Lake에 MERGE (동시성 충돌 방지)
        # 테이블은 이미 존재한다고 가정하고 MERGE 실행
        lifecycle_records.createOrReplaceTempView("new_lifecycle_events")

        merge_sql = f"""
        MERGE INTO delta.`{self.lifecycle_path}` AS target
        USING new_lifecycle_events AS source
        ON target.global_event_id = source.global_event_id AND target.event_type = source.event_type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(merge_sql)
        tracked_count = lifecycle_records.count()
        print(f"Tracked {tracked_count} events in batch {batch_id}")
        return tracked_count

    def mark_silver_processing_complete(self, event_ids: list, batch_id: str):
        """Silver 처리 완료 상태로 업데이트"""
        if not event_ids:
            return 0

        current_time = datetime.now(timezone.utc)

        event_df = self.spark.createDataFrame(
            [(event_id,) for event_id in event_ids], ["global_event_id"]
        )
        event_df.createOrReplaceTempView("silver_completed_events_temp")

        merge_sql = f"""
        MERGE INTO delta.`{self.lifecycle_path}` AS lifecycle
        USING silver_completed_events_temp AS events
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type = 'EVENT'
        WHEN MATCHED THEN
        UPDATE SET
            audit.silver_processing_end_time = '{current_time}',
            status = 'SILVER_COMPLETE'
        """

        self.spark.sql(merge_sql)
        return len(event_ids)

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
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type = 'EVENT'
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
        ON lifecycle.global_event_id = events.global_event_id AND lifecycle.event_type = 'EVENT'
        WHEN MATCHED THEN
        UPDATE SET
            audit.postgres_migration_end_time = '{current_time}',
            status = 'POSTGRES_COMPLETE'
        """

        self.spark.sql(merge_sql)
        return len(event_ids)

    def bulk_update_status(self, from_status: str, to_status: str):
        """특정 상태의 모든 EVENT 타입 이벤트를 다른 상태로 업데이트"""
        current_time = datetime.now(timezone.utc)

        update_sql = f"""
        UPDATE delta.`{self.lifecycle_path}`
        SET status = '{to_status}',
            audit.postgres_migration_end_time = '{current_time}'
        WHERE status = '{from_status}' AND event_type = 'EVENT'
        """

        self.spark.sql(update_sql)

        # 업데이트된 레코드 수 반환
        count_sql = f"""
        SELECT COUNT(*) as count FROM delta.`{self.lifecycle_path}`
        WHERE status = '{to_status}' AND event_type = 'EVENT'
        """
        result = self.spark.sql(count_sql).collect()[0]
        return result["count"]

    def expire_old_waiting_events(self, hours_threshold: int = 24):
        """24시간 이상 대기 중인 이벤트들을 EXPIRED로 변경"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

        # SQL UPDATE로 만료 처리 (EVENT 타입만)
        update_sql = f"""
        UPDATE delta.`{self.lifecycle_path}`
        SET status = 'EXPIRED'
        WHERE status = 'WAITING'
        AND event_type = 'EVENT'
        AND audit.bronze_arrival_time < '{cutoff_time}'
        """

        # 업데이트 전 카운트 확인
        count_sql = f"""
        SELECT COUNT(*) as expired_count
        FROM delta.`{self.lifecycle_path}`
        WHERE status = 'WAITING'
        AND event_type = 'EVENT'
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
            col("bronze_arrival_time") >= lit(cutoff_time)
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
