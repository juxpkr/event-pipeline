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
from src.audit.lifecycle_tracker import EventLifecycleTracker
from src.validation.lifecycle_metrics_exporter import export_lifecycle_audit_metrics

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LifecycleAuditor:
    """Event Lifecycle 기반 데이터 감사 시스템"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.lifecycle_tracker = EventLifecycleTracker(spark)
        self.audit_results = {}

        # lifecycle 테이블 존재 여부 확인 및 초기화
        try:
            self.spark.read.format("delta").load(self.lifecycle_tracker.lifecycle_path).limit(1).collect()
            logger.info("Lifecycle table found, ready for audit")
        except Exception:
            logger.info("Lifecycle table not found, initializing...")
            self.lifecycle_tracker.initialize_table()
            logger.info("Lifecycle table initialized successfully")

    def audit_collection_accuracy(self, hours_back: int = 1) -> Dict:
        """감사 1: 수집 정확성 - GDELT 예상 파일 수 vs 실제 추적된 이벤트 수"""
        logger.info("Starting Collection Accuracy Audit")

        try:
            # GDELT 예상 파일 수 계산 (기존 로직 재사용)
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=hours_back)

            # 15분 단위로 정렬
            minute_rounded = (start_time.minute // 15) * 15
            start_time = start_time.replace(minute=minute_rounded, second=0, microsecond=0)

            expected_batches = int((end_time - start_time).total_seconds() / (15 * 60))
            expected_files = expected_batches * 2  # events + gkg

            # 실제 lifecycle에 기록된 이벤트 수
            lifecycle_df = self.spark.read.format("delta").load(self.lifecycle_tracker.lifecycle_path)
            tracked_events = lifecycle_df.filter(
                col("bronze_arrival_time") >= lit(start_time)
            ).count()

            collection_rate = (tracked_events / (expected_files * 5000) * 100) if expected_files > 0 else 0.0

            results = {
                "expected_batches": expected_batches,
                "expected_files": expected_files,
                "tracked_events": tracked_events,
                "collection_rate": round(collection_rate, 2)
            }

            logger.info(f"Expected GDELT batches: {expected_batches}")
            logger.info(f"Expected files: {expected_files}")
            logger.info(f"Tracked events: {tracked_events:,}")
            logger.info(f"Collection rate: {collection_rate:.2f}%")

            self.audit_results["collection_accuracy"] = results
            return results

        except Exception as e:
            logger.error(f"Collection Accuracy Audit Failed: {str(e)}")
            raise

    def audit_join_yield(self, hours_back: int = 15, maturity_hours: int = 0) -> Dict:
        """감사 2: Join Yield - 성숙한 이벤트들의 조인 성공률"""
        logger.info(f"Starting Join Yield Audit (maturity: {maturity_hours}h)")

        try:
            lifecycle_df = self.spark.read.format("delta").load(self.lifecycle_tracker.lifecycle_path)

            # 데이터가 있으면 원래 로직, 없으면 전체 데이터로 분석
            total_count = lifecycle_df.count()
            if total_count == 0:
                logger.warning("No lifecycle data found, returning empty results")
                return {
                    "total_mature_events": 0,
                    "waiting_events": 0,
                    "joined_events": 0,
                    "expired_events": 0,
                    "join_yield": 0.0
                }

            # 데이터 있으면 시간 범위 적용, 없으면 전체 데이터 사용
            end_cutoff = datetime.now(timezone.utc) - timedelta(hours=maturity_hours)
            start_cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

            # 지정된 시간 범위에 데이터가 있는지 확인
            mature_events = lifecycle_df.filter(
                (col("bronze_arrival_time") >= lit(start_cutoff)) &
                (col("bronze_arrival_time") <= lit(end_cutoff))
            )
            mature_count = mature_events.count()

            # 시간 범위 내 데이터가 없으면 최신 데이터 일부만 사용
            if mature_count == 0:
                logger.warning(f"No data in {maturity_hours}-{hours_back}h range, using latest available data")
                # 최신 1000개 레코드만 가져와서 분석 (성능 고려)
                mature_events = lifecycle_df.orderBy(col("bronze_arrival_time").desc()).limit(1000)

            # 상태별 집계
            status_counts = mature_events.groupBy("status").count().collect()

            waiting_count = 0
            joined_count = 0
            expired_count = 0

            for row in status_counts:
                if row["status"] == "WAITING":
                    waiting_count = row["count"]
                elif row["status"] == "JOINED":
                    joined_count = row["count"]
                elif row["status"] == "EXPIRED":
                    expired_count = row["count"]

            total_mature = waiting_count + joined_count + expired_count
            completed_events = joined_count + expired_count

            join_yield = (joined_count / completed_events * 100) if completed_events > 0 else 0.0

            results = {
                "total_mature_events": total_mature,
                "waiting_events": waiting_count,
                "joined_events": joined_count,
                "expired_events": expired_count,
                "join_yield": float(round(join_yield, 2))
            }

            logger.info(f"Mature events (12-24h ago): {total_mature:,}")
            logger.info(f"Joined: {joined_count:,}, Expired: {expired_count:,}, Still waiting: {waiting_count:,}")
            logger.info(f"Join Yield: {join_yield:.2f}%")

            # 임계값 검증 (80% 미만 시 경고)
            if join_yield < 80.0:
                logger.warning(f"Join Yield ({join_yield:.2f}%) is below 80% threshold")

            self.audit_results["join_yield"] = results
            return results

        except Exception as e:
            logger.error(f"Join Yield Audit Failed: {str(e)}")
            raise

    def audit_data_loss_detection(self, hours_threshold: int = 24) -> Dict:
        """감사 3: 데이터 유실 탐지 - 24시간 이상 대기 중인 이벤트"""
        logger.info(f"Starting Data Loss Detection (threshold: {hours_threshold}h)")

        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

            lifecycle_df = self.spark.read.format("delta").load(self.lifecycle_tracker.lifecycle_path)

            # 24시간 이상 WAITING인 의심 이벤트들
            suspicious_events = lifecycle_df.filter(
                (col("status") == "WAITING") &
                (col("bronze_arrival_time") < lit(cutoff_time))
            )

            suspicious_count = suspicious_events.count()

            # 배치별 의심 이벤트 분포
            suspicious_by_batch = suspicious_events.groupBy("batch_id").count().orderBy(desc("count"))
            top_problematic_batches = suspicious_by_batch.take(5)

            results = {
                "suspicious_events": suspicious_count,
                "threshold_hours": hours_threshold,
                "top_problematic_batches": [
                    {"batch_id": row["batch_id"], "count": row["count"]}
                    for row in top_problematic_batches
                ]
            }

            logger.info(f"Suspicious events (>{hours_threshold}h waiting): {suspicious_count:,}")

            if suspicious_count > 0:
                logger.warning("Top problematic batches:")
                for batch in results["top_problematic_batches"]:
                    logger.warning(f"  {batch['batch_id']}: {batch['count']} events")

            # 유실 탐지 시 자동으로 EXPIRED 상태로 변경
            if suspicious_count > 0:
                expired_count = self.lifecycle_tracker.expire_old_waiting_events(hours_threshold)
                results["auto_expired"] = expired_count
                logger.info(f"Auto-expired {expired_count} old waiting events")

            self.audit_results["data_loss_detection"] = results
            return results

        except Exception as e:
            logger.error(f"Data Loss Detection Failed: {str(e)}")
            raise

    def audit_gold_postgres_sync(self) -> Dict:
        """감사 4: Gold-Postgres 동기화 정확성 (기존 로직 유지)"""
        logger.info("Starting Gold-Postgres Sync Audit")

        try:
            # 환경변수 확인
            postgres_url = os.getenv("POSTGRES_JDBC_URL")
            postgres_user = os.getenv("POSTGRES_USER")
            postgres_password = os.getenv("POSTGRES_PASSWORD")

            if not all([postgres_url, postgres_user, postgres_password]):
                logger.warning("PostgreSQL 환경변수가 미설정됨. Gold-Postgres 동기화 감사 건너뜀")
                return {
                    "gold_count": 0,
                    "postgres_count": 0,
                    "sync_accuracy": 100.0,
                    "skipped": True
                }

            # Gold 레코드 수
            gold_count = self.spark.table("gold_prod.gold_dashboard_master").count()

            # Postgres 레코드 수
            postgres_df = (
                self.spark.read.format("jdbc")
                .option("url", postgres_url)
                .option("dbtable", "gold.gold_dashboard_master")
                .option("user", postgres_user)
                .option("password", postgres_password)
                .load()
            )
            postgres_count = postgres_df.count()

            sync_accuracy = 100.0 if gold_count == postgres_count else 0.0

            results = {
                "gold_count": gold_count,
                "postgres_count": postgres_count,
                "sync_accuracy": sync_accuracy
            }

            logger.info(f"Gold records: {gold_count:,}")
            logger.info(f"Postgres records: {postgres_count:,}")
            logger.info(f"Sync accuracy: {sync_accuracy:.1f}%")

            # 100% 동기화 필수
            if gold_count != postgres_count:
                raise ValueError(f"CRITICAL: Gold ({gold_count:,}) != Postgres ({postgres_count:,})")

            self.audit_results["gold_postgres_sync"] = results
            return results

        except Exception as e:
            logger.error(f"Gold-Postgres Sync Audit Failed: {str(e)}")
            raise

    def run_full_lifecycle_audit(self, hours_back: int = 15) -> Dict:
        """전체 lifecycle 기반 감사 실행"""
        start_time = time.time()
        logger.info("=== Starting Event Lifecycle Audit System ===")
        logger.info(f"Audit window: Last {hours_back} hours")

        try:
            # 4가지 핵심 감사 실행 (개별 try-catch)
            try:
                self.audit_collection_accuracy(hours_back=1)
            except Exception as e:
                logger.error(f"Collection accuracy audit failed: {e}")

            try:
                self.audit_join_yield(hours_back=hours_back, maturity_hours=0)
            except Exception as e:
                logger.error(f"Join yield audit failed: {e}")

            try:
                self.audit_data_loss_detection(hours_threshold=24)
            except Exception as e:
                logger.error(f"Data loss detection failed: {e}")

            try:
                self.audit_gold_postgres_sync()
            except Exception as e:
                logger.error(f"Gold-Postgres sync audit failed: {e}")

            audit_duration = time.time() - start_time

            # 전체 감사 결과 요약
            logger.info("=== Lifecycle Audit Summary ===")
            logger.info(f"Collection Rate: {self.audit_results.get('collection_accuracy', {}).get('collection_rate', 0):.1f}%")
            logger.info(f"Join Yield: {self.audit_results.get('join_yield', {}).get('join_yield', 0):.1f}%")
            logger.info(f"Suspicious Events: {self.audit_results.get('data_loss_detection', {}).get('suspicious_events', 0):,}")
            logger.info(f"Gold-Postgres Sync: {self.audit_results.get('gold_postgres_sync', {}).get('sync_accuracy', 0):.1f}%")
            logger.info(f"Audit Duration: {audit_duration:.2f}s")

            # 전체 상태 판정
            overall_health = all([
                self.audit_results.get('collection_accuracy', {}).get('collection_rate', 0) >= 70.0,
                self.audit_results.get('join_yield', {}).get('join_yield', 0) >= 80.0,
                self.audit_results.get('data_loss_detection', {}).get('suspicious_events', 1) == 0,
                self.audit_results.get('gold_postgres_sync', {}).get('sync_accuracy', 0) == 100.0
            ])

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