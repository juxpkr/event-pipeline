"""
Event Lifecycle Tracker
이벤트 생명주기 추적을 위한 Delta Lake 관리 모듈
"""

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

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
                StructField("bronze_arrival_time", TimestampType(), False),
                StructField("detailed_joined_time", TimestampType(), True),
                StructField("status", StringType(), False),  # WAITING, JOINED, EXPIRED
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

    def track_bronze_arrival(self, events_df: DataFrame, batch_id: str):
        """Bronze 도착 이벤트들을 lifecycle에 기록"""
        current_time = datetime.now()

        # 이벤트 ID 추출 및 메타데이터 생성
        lifecycle_records = (
            events_df.select("global_event_id")
            .distinct()
            .withColumn("bronze_arrival_time", lit(current_time))
            .withColumn("detailed_joined_time", lit(None).cast(TimestampType()))
            .withColumn("status", lit("WAITING"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("year", year(lit(current_time)))
            .withColumn("month", month(lit(current_time)))
            .withColumn("day", dayofmonth(lit(current_time)))
            .withColumn("hour", hour(lit(current_time)))
        )

        # Delta Lake에 APPEND
        lifecycle_records.write.format("delta").mode("append").save(self.lifecycle_path)

        tracked_count = lifecycle_records.count()
        print(f"Tracked {tracked_count} events in batch {batch_id}")
        return tracked_count

    def mark_events_joined(self, joined_event_ids: list, batch_id: str):
        """조인 성공한 이벤트들을 JOINED로 업데이트"""
        if not joined_event_ids:
            return 0

        current_time = datetime.now()

        # Delta Table 로드
        delta_table = DeltaTable.forPath(self.spark, self.lifecycle_path)

        # 조인된 이벤트들 DataFrame 생성
        joined_df = (
            self.spark.createDataFrame(
                [(event_id,) for event_id in joined_event_ids], ["global_event_id"]
            )
            .withColumn("detailed_joined_time", lit(current_time))
            .withColumn("status", lit("JOINED"))
        )

        # MERGE로 업데이트
        delta_table.alias("lifecycle").merge(
            joined_df.alias("joined"),
            "lifecycle.global_event_id = joined.global_event_id",
        ).whenMatched().updateAll().execute()

        updated_count = len(joined_event_ids)
        print(f"Marked {updated_count} events as JOINED in batch {batch_id}")
        return updated_count

    def expire_old_waiting_events(self, hours_threshold: int = 24):
        """24시간 이상 대기 중인 이벤트들을 EXPIRED로 변경"""
        cutoff_time = datetime.now() - timedelta(hours=hours_threshold)

        delta_table = DeltaTable.forPath(self.spark, self.lifecycle_path)

        # 오래된 WAITING 이벤트들을 EXPIRED로 변경
        expired_count = delta_table.update(
            condition=col("status")
            == "WAITING" & (col("bronze_arrival_time") < lit(cutoff_time)),
            set={"status": lit("EXPIRED")},
        )

        print(f"Expired {expired_count} events older than {hours_threshold} hours")
        return expired_count

    def get_lifecycle_stats(self, hours_back: int = 24) -> dict:
        """최근 N시간 lifecycle 통계 조회"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

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
