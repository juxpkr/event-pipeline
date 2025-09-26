"""
Event Lifecycle Tracker
이벤트 생명주기 추적을 위한 Delta Lake 관리 모듈
"""

import os
import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *


LIFECYCLE_PATH = "s3a://warehouse/audit/lifecycle"


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
                StructField(
                    "audit",
                    StructType(
                        [
                            StructField("bronze_arrival_time", TimestampType(), False),
                            StructField(
                                "silver_processing_end_time", TimestampType(), True
                            ),
                            StructField(
                                "gold_processing_end_time", TimestampType(), True
                            ),
                            StructField(
                                "postgres_migration_end_time", TimestampType(), True
                            ),
                        ]
                    ),
                    False,
                ),
                StructField(
                    "status", StringType(), False
                ),  # WAITING, SILVER_COMPLETE, GOLD_COMPLETE, POSTGRES_COMPLETE
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
            "year", "month", "day", "hour", "event_type"
        ).save(self.lifecycle_path)

        print(f"Event lifecycle table initialized at {self.lifecycle_path}")

    def track_bronze_arrival(
        self, events_df: DataFrame, batch_id: str, event_type: str
    ):
        """Bronze 도착 이벤트들을 lifecycle에 기록 (DeltaTable.merge() 사용)"""
        current_time = datetime.now(timezone.utc)
        from pyspark.sql.functions import struct, lit, year, month, dayofmonth, hour
        from pyspark.sql.types import TimestampType

        lifecycle_records = (
            events_df.select("global_event_id")
            .distinct()
            .withColumn("event_type", lit(event_type))
            .withColumn(
                "audit",
                struct(
                    lit(current_time).alias("bronze_arrival_time"),
                    lit(None).cast(TimestampType()).alias("silver_processing_end_time"),
                    lit(None).cast(TimestampType()).alias("gold_processing_end_time"),
                    lit(None)
                    .cast(TimestampType())
                    .alias("postgres_migration_end_time"),
                ),
            )
            .withColumn("status", lit("WAITING"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn("year", year(lit(current_time)))
            .withColumn("month", month(lit(current_time)))
            .withColumn("day", dayofmonth(lit(current_time)))
            .withColumn("hour", hour(lit(current_time)))
        )

        # event_type에 따라 저장 경로를 동적으로 결정
        staging_path = f"s3a://warehouse/audit/lifecycle_staging_{event_type.lower()}"

        try:
            # Delta 테이블이 존재하는지 먼저 확인
            if DeltaTable.isDeltaTable(self.spark, staging_path):
                # 테이블이 존재하면, MERGE 수행
                target_delta_table = DeltaTable.forPath(self.spark, staging_path)
                target_delta_table.alias("target").merge(
                    source=lifecycle_records.alias("source"),
                    condition="target.global_event_id = source.global_event_id",
                ).whenNotMatchedInsertAll().execute()
            else:
                # 테이블이 없으면, 이 배치의 데이터로 테이블을 '생성'
                logger.info(
                    f"Staging table not found at {staging_path}. Creating it for the first time."
                )
                lifecycle_records.write.format("delta").partitionBy(
                    "year", "month", "day", "hour", "event_type"
                ).save(staging_path)

            tracked_count = lifecycle_records.count()
            logger.info(
                f"Tracked {tracked_count} events in staging table: {staging_path}"
            )
            return tracked_count

        except Exception as e:
            logger.error(
                f"Failed to track bronze arrival for batch {batch_id} at {staging_path}. Error: {str(e)}"
            )
            raise e
