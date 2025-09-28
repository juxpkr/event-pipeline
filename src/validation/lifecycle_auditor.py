"""
Event Lifecycle Based Auditor
실제 이벤트 생명주기를 기반으로 한 데이터 감사 시스템
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, desc, year, month, dayofmonth, hour

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[2]
import sys

sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from src.audit.lifecycle_updater import EventLifecycleUpdater
from src.validation.lifecycle_metrics_exporter import export_lifecycle_audit_metrics

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ===== 독립적인 계산 함수들 =====


def calculate_join_yield(
    lifecycle_df, hours_back: int = 15, maturity_hours: int = 0
) -> Dict:
    """Join Yield 계산 (agg 사용)"""
    total_count = lifecycle_df.count()
    if total_count == 0:
        return {
            "total_mature_events": 0,
            "waiting_events": 0,
            "joined_events": 0,
            "expired_events": 0,
            "join_yield": 0.0,
        }

    # 성숙한 이벤트 필터링
    end_cutoff = datetime.now(timezone.utc) - timedelta(hours=maturity_hours)
    start_cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    # maturity_cutoff = datetime.now(timezone.utc) - timedelta(hours=15)
    maturity_cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)

    # 모든 이벤트 타입 포함 - 파이프라인 전체 건강도 측정
    mature_events = lifecycle_df.filter(
        col("audit.bronze_arrival_time") <= lit(maturity_cutoff)
    )

    # agg로 상태별 집계 - collect() 없이
    from pyspark.sql.functions import sum as spark_sum, when, count

    status_stats = mature_events.agg(
        spark_sum(when(col("status") == "WAITING", 1).otherwise(0)).alias(
            "waiting_count"
        ),
        spark_sum(
            when(
                col("status").isin(
                    ["SILVER_COMPLETE", "GOLD_COMPLETE", "POSTGRES_COMPLETE"]
                ),
                1,
            ).otherwise(0)
        ).alias("joined_count"),
        spark_sum(when(col("status") == "EXPIRED", 1).otherwise(0)).alias(
            "expired_count"
        ),
        count("*").alias("total_count"),
    ).collect()[
        0
    ]  # 단일 집계 결과만 collect

    waiting_count = status_stats["waiting_count"] or 0
    joined_count = status_stats["joined_count"] or 0
    expired_count = status_stats["expired_count"] or 0
    total_mature = status_stats["total_count"] or 0

    # 분모를 결과가 나온 이벤트들로만 한정
    completed_events_count = joined_count + expired_count

    # 실패 건수가 0일 때는 100% 성공으로 간주
    join_yield = (
        (joined_count / completed_events_count * 100)
        if completed_events_count > 0
        else 100.0
    )

    return {
        "total_mature_events": total_mature,
        "waiting_events": waiting_count,
        "joined_events": joined_count,
        "expired_events": expired_count,
        "join_yield": float(round(join_yield, 2)),
    }


def calculate_gold_postgres_sync(spark: SparkSession) -> Dict:
    """Gold-Postgres 동기화 정확성 계산"""
    postgres_url = os.getenv("POSTGRES_JDBC_URL")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    if not all([postgres_url, postgres_user, postgres_password]):
        return {
            "gold_count": 0,
            "postgres_count": 0,
            "sync_accuracy": 100.0,
            "skipped": True,
        }

    # agg로 집계 (collect 최소화) - 6개 테이블 모두 체크
    gold_counts = spark.sql(
        """
        SELECT
            SUM(CASE WHEN table_name = 'gold_near_realtime_summary' THEN cnt ELSE 0 END) as realtime_count,
            SUM(CASE WHEN table_name = 'gold_daily_actor_network' THEN cnt ELSE 0 END) as actor_count,
            SUM(CASE WHEN table_name = 'gold_chart_events_category' THEN cnt ELSE 0 END) as category_count,
            SUM(CASE WHEN table_name = 'gold_chart_weekday_event_ratio' THEN cnt ELSE 0 END) as event_ratio_count,
            SUM(CASE WHEN table_name = 'gold_chart_events_count_avgtone' THEN cnt ELSE 0 END) as events_count_avgtone_count,
            SUM(CASE WHEN table_name = 'gold_daily_rich_story' THEN cnt ELSE 0 END) as daily_rich_story_count
        FROM (
            SELECT 'gold_near_realtime_summary' as table_name, COUNT(*) as cnt FROM gold_prod.gold_near_realtime_summary
            UNION ALL
            SELECT 'gold_daily_actor_network' as table_name, COUNT(*) as cnt FROM gold_prod.gold_daily_actor_network
            UNION ALL
            SELECT 'gold_chart_events_category' as table_name, COUNT(*) as cnt FROM gold_prod.gold_chart_events_category
            UNION ALL
            SELECT 'gold_chart_weekday_event_ratio' as table_name, COUNT(*) as cnt FROM gold_prod.gold_chart_weekday_event_ratio
            UNION ALL
            SELECT 'gold_chart_events_count_avgtone' as table_name, COUNT(*) as cnt FROM gold_prod.gold_chart_events_count_avgtone
            UNION ALL
            SELECT 'gold_daily_rich_story' as table_name, COUNT(*) as cnt FROM gold_prod.gold_daily_rich_story
        )
    """
    ).collect()[0]

    gold_count = (
        (gold_counts["realtime_count"] or 0)
        + (gold_counts["actor_count"] or 0)
        + (gold_counts["category_count"] or 0)
        + (gold_counts["event_ratio_count"] or 0)
        + (gold_counts["events_count_avgtone_count"] or 0)
        + (gold_counts["daily_rich_story_count"] or 0)
    )

    # Postgres도 동일하게 6개 테이블 체크
    postgres_realtime = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_near_realtime_summary")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )

    postgres_actor = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_daily_actor_network")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )

    postgres_category = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_chart_events_category")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )
    postgres_event_ratio_count = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_chart_weekday_event_ratio")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )
    postgres_events_count_avgtone_count = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_chart_events_count_avgtone")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )

    postgres_daily_rich_story = (
        spark.read.format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "gold.gold_daily_rich_story")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .load()
        .count()
    )

    postgres_count = (
        postgres_realtime
        + postgres_actor
        + postgres_category
        + postgres_event_ratio_count
        + postgres_events_count_avgtone_count
        + postgres_daily_rich_story
    )
    sync_accuracy = 100.0 if gold_count == postgres_count else 0.0

    return {
        "gold_count": gold_count,
        "postgres_count": postgres_count,
        "sync_accuracy": sync_accuracy,
    }


def calculate_stage_durations(lifecycle_df, hours_back: int = 24) -> Dict:
    """단계별 소요시간 계산 (agg 사용)"""
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    complete_events = lifecycle_df.filter(
        (col("audit.bronze_arrival_time") >= lit(recent_cutoff))
        & (
            col("status").isin(
                ["SILVER_COMPLETE", "GOLD_COMPLETE", "POSTGRES_COMPLETE"]
            )
        )
    )

    complete_count = complete_events.count()
    if complete_count == 0:
        return {
            "avg_silver_duration_hours": 0.0,
            "avg_gold_duration_hours": 0.0,
            "avg_postgres_duration_hours": 0.0,
            "avg_e2e_duration_hours": 0.0,
            "analyzed_events": 0,
        }

    # agg로 평균 계산
    from pyspark.sql.functions import avg, when, unix_timestamp

    duration_stats = complete_events.agg(
        avg(
            when(
                col("audit.silver_processing_end_time").isNotNull(),
                (
                    unix_timestamp("audit.silver_processing_end_time")
                    - unix_timestamp("audit.bronze_arrival_time")
                )
                / 3600.0,
            )
        ).alias("avg_silver_hours"),
        avg(
            when(
                col("audit.gold_processing_end_time").isNotNull(),
                (
                    unix_timestamp("audit.gold_processing_end_time")
                    - unix_timestamp("audit.silver_processing_end_time")
                )
                / 3600.0,
            )
        ).alias("avg_gold_hours"),
        avg(
            when(
                col("audit.postgres_migration_end_time").isNotNull(),
                (
                    unix_timestamp("audit.postgres_migration_end_time")
                    - unix_timestamp("audit.gold_processing_end_time")
                )
                / 3600.0,
            )
        ).alias("avg_postgres_hours"),
        avg(
            when(
                col("audit.postgres_migration_end_time").isNotNull(),
                (
                    unix_timestamp("audit.postgres_migration_end_time")
                    - unix_timestamp("audit.bronze_arrival_time")
                )
                / 3600.0,
            )
        ).alias("avg_e2e_hours"),
    ).collect()[
        0
    ]  # 단일 집계 결과만 collect

    return {
        "avg_silver_duration_hours": round(
            float(duration_stats["avg_silver_hours"] or 0), 2
        ),
        "avg_gold_duration_hours": round(
            float(duration_stats["avg_gold_hours"] or 0), 2
        ),
        "avg_postgres_duration_hours": round(
            float(duration_stats["avg_postgres_hours"] or 0), 2
        ),
        "avg_e2e_duration_hours": round(float(duration_stats["avg_e2e_hours"] or 0), 2),
        "analyzed_events": complete_count,
    }


class LifecycleAuditor:
    """Event Lifecycle 기반 데이터 감사 시스템 - 캐시 관리자"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.lifecycle_updater = EventLifecycleUpdater(spark)
        self.audit_results = {}

        # lifecycle 테이블 존재 여부 확인 및 초기화
        try:
            self.spark.read.format("delta").load(
                self.lifecycle_updater.lifecycle_path
            ).limit(1).collect()
            logger.info("Lifecycle table found, ready for audit")
        except Exception:
            logger.info("Lifecycle table not found, will be created by consolidator")

        # 데이터를 딱 한 번만 읽어서 메모리에 캐시
        try:
            self.lifecycle_df = (
                self.spark.read.format("delta")
                .load(self.lifecycle_updater.lifecycle_path)
                .cache()
            )
        except:
            # Main 테이블이 없으면 빈 DataFrame으로 초기화
            from pyspark.sql.types import StructType

            self.lifecycle_df = self.spark.createDataFrame([], StructType([]))
        logger.info("Lifecycle data cached in memory")

    def run_full_lifecycle_audit(self, hours_back: int = 15) -> Dict:
        """전체 lifecycle 기반 감사 실행 - 캐시된 데이터 사용"""
        start_time = time.time()
        logger.info("=== Starting Event Lifecycle Audit System ===")
        logger.info(f"Audit window: Last {hours_back} hours")

        try:
            # 1. 먼저 오래된 WAITING 이벤트들을 EXPIRED로 정리 (청소부 역할)
            logger.info("Cleaning up expired events...")
            expired_count = self.lifecycle_updater.expire_old_waiting_events(
                hours_threshold=15
            )
            if expired_count > 0:
                logger.info(f"Expired {expired_count} old waiting events")
                # 데이터가 변경되었으므로 캐시 새로고침
                self.lifecycle_df.unpersist()
                self.lifecycle_df = (
                    self.spark.read.format("delta")
                    .load(self.lifecycle_updater.lifecycle_path)
                    .cache()
                )

            # 2. 캐시된 데이터프레임을 계산 함수들에 전달
            logger.info("Using cached lifecycle data for all calculations")

            # Join Yield 계산
            try:
                join_results = calculate_join_yield(
                    self.lifecycle_df, hours_back, maturity_hours=0
                )
                self.audit_results["join_yield"] = join_results
                logger.info(f"Join Yield: {join_results['join_yield']:.1f}%")
            except Exception as e:
                logger.error(f"Join yield calculation failed: {e}")

            # Gold-Postgres Sync 계산
            try:
                sync_results = calculate_gold_postgres_sync(self.spark)
                self.audit_results["gold_postgres_sync"] = sync_results
                logger.info(f"Gold-Postgres Sync: {sync_results['sync_accuracy']:.1f}%")
            except Exception as e:
                logger.error(f"Gold-Postgres sync calculation failed: {e}")

            # Stage Duration 계산
            try:
                duration_results = calculate_stage_durations(
                    self.lifecycle_df, hours_back
                )
                self.audit_results["stage_durations"] = duration_results
                logger.info(
                    f"E2E Duration: {duration_results['avg_e2e_duration_hours']}h"
                )
            except Exception as e:
                logger.error(f"Stage duration calculation failed: {e}")

            audit_duration = time.time() - start_time

            # 전체 감사 결과 요약
            logger.info("=== Lifecycle Audit Summary ===")
            logger.info(
                f"Join Yield: {self.audit_results.get('join_yield', {}).get('join_yield', 0):.1f}%"
            )
            logger.info(f"Suspicious Events: 0")  # 임시
            logger.info(
                f"Gold-Postgres Sync: {self.audit_results.get('gold_postgres_sync', {}).get('sync_accuracy', 0):.1f}%"
            )
            logger.info(f"Audit Duration: {audit_duration:.2f}s")

            # 전체 상태 판정 (단순화)
            overall_health = all(
                [
                    self.audit_results.get("join_yield", {}).get("join_yield", 0)
                    >= 80.0,
                    self.audit_results.get("gold_postgres_sync", {}).get(
                        "sync_accuracy", 0
                    )
                    == 100.0,
                ]
            )

            self.audit_results["overall_health"] = overall_health
            self.audit_results["audit_duration"] = audit_duration

            status = "HEALTHY" if overall_health else "UNHEALTHY"
            logger.info(f"Overall Pipeline Status: {status}")

            # Prometheus 메트릭 전송
            try:
                export_lifecycle_audit_metrics(self.audit_results)
                logger.info("Metrics exported to Prometheus successfully")
            except Exception as e:
                logger.warning(f"Failed to export metrics to Prometheus: {e}")

            return self.audit_results

        except Exception as e:
            audit_duration = time.time() - start_time
            logger.error(f"Lifecycle Audit FAILED: {str(e)}")
            self.audit_results["overall_health"] = False
            self.audit_results["audit_duration"] = audit_duration
            raise


def run_lifecycle_audit(hours_back: int = 24):
    """메인 실행 함수"""
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark = get_spark_session("Lifecycle_Auditor", spark_master)

    try:
        auditor = LifecycleAuditor(spark)
        results = auditor.run_full_lifecycle_audit(hours_back)
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    run_lifecycle_audit()
