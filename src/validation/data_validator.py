import os
import sys
import logging
import time
import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

# Prometheus metrics (런타임에 import 시도)
try:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    PROMETHEUS_AVAILABLE = True
except ImportError:
    logger.warning(
        "prometheus_client not available - metrics will be sent via HTTP API"
    )
    PROMETHEUS_AVAILABLE = False

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class PrometheusMetricsExporter:
    """Prometheus Pushgateway로 메트릭 전송 관리"""

    def __init__(self, pushgateway_url: str = None, job_name: str = "data_audit"):
        self.pushgateway_url = pushgateway_url or os.getenv(
            "PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091"
        )
        self.job_name = job_name
        self.registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self._setup_metrics()

    def _setup_metrics(self):
        """Prometheus 메트릭 정의"""
        if not PROMETHEUS_AVAILABLE:
            return

        # Source vs Bronze 수집률 메트릭
        self.source_count_total = Gauge(
            "gdelt_source_count_total",
            "Expected number of files from GDELT server",
            ["time_window"],
            registry=self.registry,
        )
        self.collection_rate = Gauge(
            "gdelt_collection_rate",
            "Collection rate from Source to Bronze (percentage)",
            ["time_window"],
            registry=self.registry,
        )

        # Bronze Layer 메트릭
        self.bronze_events_total = Gauge(
            "gdelt_bronze_events_total",
            "Total number of events in Bronze layer",
            ["time_window"],
            registry=self.registry,
        )
        self.bronze_mentions_total = Gauge(
            "gdelt_bronze_mentions_total",
            "Total number of mentions in Bronze layer",
            ["time_window"],
            registry=self.registry,
        )
        self.bronze_gkg_total = Gauge(
            "gdelt_bronze_gkg_total",
            "Total number of GKG records in Bronze layer",
            ["time_window"],
            registry=self.registry,
        )

        # 조인 성공률 메트릭
        self.join_success_rate = Gauge(
            "gdelt_bronze_silver_join_success_rate",
            "Success rate of Bronze to Silver join (percentage)",
            ["time_window"],
            registry=self.registry,
        )
        self.bronze_distinct_events = Gauge(
            "gdelt_bronze_distinct_events",
            "Distinct event IDs in Bronze layer",
            ["time_window"],
            registry=self.registry,
        )
        self.silver_distinct_events = Gauge(
            "gdelt_silver_distinct_events",
            "Distinct event IDs in Silver layer",
            ["time_window"],
            registry=self.registry,
        )

        # 변환 무결성 메트릭
        self.transformation_ratio = Gauge(
            "gdelt_silver_gold_transformation_ratio",
            "Silver to Gold transformation ratio (percentage)",
            ["time_window"],
            registry=self.registry,
        )
        self.gold_distinct_events = Gauge(
            "gdelt_gold_distinct_events",
            "Distinct event IDs in Gold layer",
            ["time_window"],
            registry=self.registry,
        )

        # 데이터 이전 정확성 메트릭
        self.transfer_accuracy = Gauge(
            "gdelt_gold_postgres_transfer_accuracy",
            "Gold to Postgres transfer accuracy (percentage)",
            ["time_window"],
            registry=self.registry,
        )
        self.gold_total_records = Gauge(
            "gdelt_gold_total_records",
            "Total records in Gold layer",
            ["time_window"],
            registry=self.registry,
        )
        self.postgres_total_records = Gauge(
            "gdelt_postgres_total_records",
            "Total records in Postgres",
            ["time_window"],
            registry=self.registry,
        )

        # 전체 감사 상태 메트릭
        self.audit_status = Gauge(
            "gdelt_audit_status",
            "Overall audit status (1=PASS, 0=FAIL)",
            ["time_window"],
            registry=self.registry,
        )
        self.audit_duration = Gauge(
            "gdelt_audit_duration_seconds",
            "Duration of audit execution in seconds",
            ["time_window"],
            registry=self.registry,
        )

    def export_metrics(
        self, audit_results: Dict, time_window: str, audit_duration: float
    ):
        """감사 결과를 Prometheus 메트릭으로 내보내기"""
        try:
            if PROMETHEUS_AVAILABLE:
                self._export_with_prometheus_client(
                    audit_results, time_window, audit_duration
                )
            else:
                self._export_with_http_api(audit_results, time_window, audit_duration)

            logger.info(f"Metrics exported to Pushgateway: {self.pushgateway_url}")

        except Exception as e:
            logger.error(f"Failed to export metrics: {str(e)}")

    def _export_with_prometheus_client(
        self, audit_results: Dict, time_window: str, audit_duration: float
    ):
        """prometheus_client 라이브러리 사용하여 메트릭 전송"""
        # Point 1: Source → Bronze Collection
        point_1 = audit_results.get("point_1", {})
        self.source_count_total.labels(time_window=time_window).set(
            point_1.get("source_count", 0)
        )
        self.collection_rate.labels(time_window=time_window).set(
            point_1.get("collection_rate", 0)
        )
        self.bronze_events_total.labels(time_window=time_window).set(
            point_1.get("bronze_events", 0)
        )
        self.bronze_mentions_total.labels(time_window=time_window).set(
            point_1.get("bronze_mentions", 0)
        )
        self.bronze_gkg_total.labels(time_window=time_window).set(
            point_1.get("bronze_gkg", 0)
        )

        # Point 2: Bronze → Silver
        point_2 = audit_results.get("point_2", {})
        self.join_success_rate.labels(time_window=time_window).set(
            point_2.get("join_success_rate", 0)
        )
        self.bronze_distinct_events.labels(time_window=time_window).set(
            point_2.get("bronze_distinct_events", 0)
        )
        self.silver_distinct_events.labels(time_window=time_window).set(
            point_2.get("silver_distinct_events", 0)
        )

        # Point 3: Silver → Gold
        point_3 = audit_results.get("point_3", {})
        self.transformation_ratio.labels(time_window=time_window).set(
            point_3.get("transformation_ratio", 0)
        )
        self.gold_distinct_events.labels(time_window=time_window).set(
            point_3.get("gold_distinct_events", 0)
        )

        # Point 4: Gold → Postgres
        point_4 = audit_results.get("point_4", {})
        self.transfer_accuracy.labels(time_window=time_window).set(
            point_4.get("transfer_accuracy", 0)
        )
        self.gold_total_records.labels(time_window=time_window).set(
            point_4.get("gold_count", 0)
        )
        self.postgres_total_records.labels(time_window=time_window).set(
            point_4.get("postgres_count", 0)
        )

        # 전체 감사 상태 (모든 임계값 통과 확인)
        audit_passed = (
            1
            if all(
                [
                    point_1.get("collection_rate", 0) >= 90,  # 수집률 90% 이상
                    point_2.get("join_success_rate", 0) >= 95,  # 조인 성공률 95% 이상
                    point_4.get("transfer_accuracy", 0) == 100,  # 이전 정확성 100%
                ]
            )
            else 0
        )
        self.audit_status.labels(time_window=time_window).set(audit_passed)
        self.audit_duration.labels(time_window=time_window).set(audit_duration)

        # Pushgateway로 전송
        push_to_gateway(self.pushgateway_url, job=self.job_name, registry=self.registry)

    def _export_with_http_api(
        self, audit_results: Dict, time_window: str, audit_duration: float
    ):
        """HTTP API 직접 호출로 메트릭 전송 (prometheus_client 없을 때)"""
        metrics_data = self._build_metrics_payload(
            audit_results, time_window, audit_duration
        )

        response = requests.post(
            f"{self.pushgateway_url}/metrics/job/{self.job_name}",
            data=metrics_data,
            headers={"Content-Type": "text/plain"},
        )
        response.raise_for_status()

    def _build_metrics_payload(
        self, audit_results: Dict, time_window: str, audit_duration: float
    ) -> str:
        """Prometheus 메트릭 형식의 페이로드 생성"""
        lines = []

        # Helper function to add metric
        def add_metric(
            name: str,
            value: float,
            labels: Dict[str, str] = None,
            help_text: str = None,
        ):
            if help_text:
                lines.append(f"# HELP {name} {help_text}")
                lines.append(f"# TYPE {name} gauge")

            label_str = ""
            if labels:
                label_pairs = [f'{k}="{v}"' for k, v in labels.items()]
                label_str = f"{{{','.join(label_pairs)}}}"

            lines.append(f"{name}{label_str} {value}")

        labels = {"time_window": time_window}

        # Point 1: Source → Bronze Collection
        point_1 = audit_results.get("point_1", {})
        add_metric(
            "gdelt_source_count_total",
            point_1.get("source_count", 0),
            labels,
            "Expected number of files from GDELT server",
        )
        add_metric(
            "gdelt_collection_rate",
            point_1.get("collection_rate", 0),
            labels,
            "Collection rate from Source to Bronze (percentage)",
        )
        add_metric(
            "gdelt_bronze_events_total",
            point_1.get("bronze_events", 0),
            labels,
            "Total number of events in Bronze layer",
        )
        add_metric(
            "gdelt_bronze_mentions_total",
            point_1.get("bronze_mentions", 0),
            labels,
            "Total number of mentions in Bronze layer",
        )
        add_metric(
            "gdelt_bronze_gkg_total",
            point_1.get("bronze_gkg", 0),
            labels,
            "Total number of GKG records in Bronze layer",
        )

        # Point 2: Bronze → Silver
        point_2 = audit_results.get("point_2", {})
        add_metric(
            "gdelt_bronze_silver_join_success_rate",
            point_2.get("join_success_rate", 0),
            labels,
            "Success rate of Bronze to Silver join (percentage)",
        )
        add_metric(
            "gdelt_bronze_distinct_events",
            point_2.get("bronze_distinct_events", 0),
            labels,
            "Distinct event IDs in Bronze layer",
        )
        add_metric(
            "gdelt_silver_distinct_events",
            point_2.get("silver_distinct_events", 0),
            labels,
            "Distinct event IDs in Silver layer",
        )

        # Point 3: Silver → Gold
        point_3 = audit_results.get("point_3", {})
        add_metric(
            "gdelt_silver_gold_transformation_ratio",
            point_3.get("transformation_ratio", 0),
            labels,
            "Silver to Gold transformation ratio (percentage)",
        )
        add_metric(
            "gdelt_gold_distinct_events",
            point_3.get("gold_distinct_events", 0),
            labels,
            "Distinct event IDs in Gold layer",
        )

        # Point 4: Gold → Postgres
        point_4 = audit_results.get("point_4", {})
        add_metric(
            "gdelt_gold_postgres_transfer_accuracy",
            point_4.get("transfer_accuracy", 0),
            labels,
            "Gold to Postgres transfer accuracy (percentage)",
        )
        add_metric(
            "gdelt_gold_total_records",
            point_4.get("gold_count", 0),
            labels,
            "Total records in Gold layer",
        )
        add_metric(
            "gdelt_postgres_total_records",
            point_4.get("postgres_count", 0),
            labels,
            "Total records in Postgres",
        )

        # 전체 감사 상태 (모든 임계값 통과 확인)
        audit_passed = (
            1
            if all(
                [
                    point_1.get("collection_rate", 0) >= 90,  # 수집률 90% 이상
                    point_2.get("join_success_rate", 0) >= 95,  # 조인 성공률 95% 이상
                    point_4.get("transfer_accuracy", 0) == 100,  # 이전 정확성 100%
                ]
            )
            else 0
        )
        add_metric(
            "gdelt_audit_status",
            audit_passed,
            labels,
            "Overall audit status (1=PASS, 0=FAIL)",
        )
        add_metric(
            "gdelt_audit_duration_seconds",
            audit_duration,
            labels,
            "Duration of audit execution in seconds",
        )

        return "\n".join(lines) + "\n"


class FourPointAuditSystem:
    """4점 감사 시스템: Source → Bronze → Silver → Gold → Postgres 데이터 무결성 검증"""

    def __init__(
        self,
        spark_session: SparkSession,
        time_window_hours: int = 1,
        enable_metrics: bool = True,
    ):
        self.spark = spark_session
        self.time_window_hours = time_window_hours
        self.audit_results = {}
        self.enable_metrics = enable_metrics
        self.metrics_exporter = PrometheusMetricsExporter() if enable_metrics else None

    def get_time_filter_condition(
        self, processed_at_column: str = "processed_at"
    ) -> str:
        """최근 N시간 데이터 필터링을 위한 조건문 생성"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=self.time_window_hours)

        return f"{processed_at_column} >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND {processed_at_column} < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"

    def audit_point_1_source_to_bronze(self) -> Dict[str, int]:
        """1차 감사: Source → Bronze 수집률 검증 (GDELT 서버와 직접 비교)"""
        logger.info("Starting Point 1 Audit: Source → Bronze Collection Rate")

        try:
            # STEP 1: GDELT 서버에서 예상되는 Source Count 계산
            source_count = self._get_gdelt_source_count()

            # STEP 2: Bronze Layer에서 실제 수집된 Count 계산
            time_filter = self.get_time_filter_condition()

            bronze_events = self.spark.sql(
                f"""
                SELECT COUNT(*) as count FROM bronze.gdelt_events
                WHERE {time_filter}
            """
            ).collect()[0]["count"]

            bronze_mentions = self.spark.sql(
                f"""
                SELECT COUNT(*) as count FROM bronze.gdelt_mentions
                WHERE {time_filter}
            """
            ).collect()[0]["count"]

            bronze_gkg = self.spark.sql(
                f"""
                SELECT COUNT(*) as count FROM bronze.gdelt_gkg
                WHERE {time_filter}
            """
            ).collect()[0]["count"]

            total_bronze = bronze_events + bronze_mentions + bronze_gkg

            # STEP 3: 수집률 계산
            collection_rate = (
                (total_bronze / source_count * 100) if source_count > 0 else 0.0
            )

            results = {
                "source_count": source_count,
                "bronze_events": bronze_events,
                "bronze_mentions": bronze_mentions,
                "bronze_gkg": bronze_gkg,
                "total_bronze": total_bronze,
                "collection_rate": round(collection_rate, 2),
            }

            logger.info(f"Expected Source Count: {source_count:,}")
            logger.info(f"Bronze Events: {bronze_events:,}")
            logger.info(f"Bronze Mentions: {bronze_mentions:,}")
            logger.info(f"Bronze GKG: {bronze_gkg:,}")
            logger.info(f"Total Bronze Records: {total_bronze:,}")
            logger.info(f"Collection Rate: {collection_rate:.2f}%")

            # 임계값 검증 (90% 미만 시 경고)
            if collection_rate < 90.0:
                logger.warning(
                    f"Collection Rate ({collection_rate:.2f}%) is below 90% threshold"
                )

            self.audit_results["point_1"] = results
            return results

        except Exception as e:
            logger.error(f"Point 1 Audit Failed: {str(e)}")
            raise

    def _get_gdelt_source_count(self) -> int:
        """GDELT 서버에서 예상되는 Source Count를 계산 (producer 로직 재사용)"""
        try:
            # 시간 범위 계산 (최근 N시간)
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=self.time_window_hours)

            # 15분 단위로 정렬 (GDELT 배치 시간에 맞춤)
            minute_rounded = (start_time.minute // 15) * 15
            start_time = start_time.replace(
                minute=minute_rounded, second=0, microsecond=0
            )

            start_time_str = start_time.isoformat()
            end_time_str = end_time.isoformat()

            # producer의 get_gdelt_urls_for_period 로직 재사용
            from src.ingestion.gdelt_producer import get_gdelt_urls_for_period

            gdelt_urls = get_gdelt_urls_for_period(
                start_time_str, end_time_str, ["events", "mentions", "gkg"]
            )

            # 각 URL이 하나의 배치를 나타내므로, URL 개수가 곧 예상되는 파일 개수
            expected_files = {
                "events": len(gdelt_urls.get("events", [])),
                "mentions": len(gdelt_urls.get("mentions", [])),
                "gkg": len(gdelt_urls.get("gkg", [])),
            }

            # 실제 파일 존재 여부를 확인하여 더 정확한 Source Count 계산
            verified_source_count = self._verify_gdelt_files_exist(gdelt_urls)

            logger.info(
                f"Expected GDELT files - Events: {expected_files['events']}, "
                f"Mentions: {expected_files['mentions']}, GKG: {expected_files['gkg']}"
            )
            logger.info(f"Verified available files: {verified_source_count}")

            return verified_source_count

        except Exception as e:
            logger.error(f"Failed to get GDELT source count: {str(e)}")
            # Fallback: URL 개수의 합계 반환
            total_expected = (
                sum(len(urls) for urls in gdelt_urls.values())
                if "gdelt_urls" in locals()
                else 0
            )
            logger.warning(f"Using fallback source count: {total_expected}")
            return total_expected

    def _verify_gdelt_files_exist(self, gdelt_urls: Dict[str, list]) -> int:
        """HTTP HEAD 요청으로 GDELT 파일 실제 존재 여부 확인"""
        available_count = 0
        total_urls = sum(len(urls) for urls in gdelt_urls.values())

        logger.info(f"Verifying {total_urls} GDELT files availability...")

        for data_type, urls in gdelt_urls.items():
            for url in urls:
                try:
                    # HEAD 요청으로 파일 존재 확인 (다운로드하지 않고 헤더만)
                    response = requests.head(url, timeout=10)
                    if response.status_code == 200:
                        available_count += 1
                    else:
                        logger.debug(
                            f"File not available: {url} (HTTP {response.status_code})"
                        )

                except requests.RequestException as e:
                    logger.debug(f"Failed to verify file: {url} - {str(e)}")
                    continue

        availability_rate = (
            (available_count / total_urls * 100) if total_urls > 0 else 0
        )
        logger.info(
            f"GDELT files availability: {available_count}/{total_urls} ({availability_rate:.1f}%)"
        )

        return available_count

    def audit_point_2_bronze_to_silver_join_rate(self) -> Dict[str, float]:
        """2차 감사: Bronze → Silver 조인 성공률 검증"""
        logger.info("Starting Point 2 Audit: Bronze → Silver Join Success Rate")

        try:
            time_filter = self.get_time_filter_condition()

            # Bronze distinct event_ids (분모)
            bronze_distinct_events = self.spark.sql(
                f"""
                SELECT COUNT(DISTINCT global_event_id) as distinct_count
                FROM bronze.gdelt_events
                WHERE {time_filter} AND global_event_id IS NOT NULL
            """
            ).collect()[0]["distinct_count"]

            # Silver distinct event_ids (분자)
            silver_distinct_events = self.spark.sql(
                f"""
                SELECT COUNT(DISTINCT global_event_id) as distinct_count
                FROM silver.gdelt_events_detailed
                WHERE {time_filter} AND global_event_id IS NOT NULL
            """
            ).collect()[0]["distinct_count"]

            # 조인 성공률 계산
            join_success_rate = (
                (silver_distinct_events / bronze_distinct_events * 100)
                if bronze_distinct_events > 0
                else 0.0
            )

            results = {
                "bronze_distinct_events": bronze_distinct_events,
                "silver_distinct_events": silver_distinct_events,
                "join_success_rate": round(join_success_rate, 2),
            }

            logger.info(f"Bronze Distinct Events: {bronze_distinct_events:,}")
            logger.info(f"Silver Distinct Events: {silver_distinct_events:,}")
            logger.info(f"Join Success Rate: {join_success_rate:.2f}%")

            # 임계값 검증 (95% 미만 시 경고)
            if join_success_rate < 95.0:
                logger.warning(
                    f"Join Success Rate ({join_success_rate:.2f}%) is below 95% threshold"
                )

            self.audit_results["point_2"] = results
            return results

        except Exception as e:
            logger.error(f"Point 2 Audit Failed: {str(e)}")
            raise

    def audit_point_3_silver_to_gold_integrity(self) -> Dict[str, int]:
        """3차 감사: Silver → Gold 변환 무결성 검증"""
        logger.info("Starting Point 3 Audit: Silver → Gold Transformation Integrity")

        try:
            time_filter = self.get_time_filter_condition()

            # Silver distinct event_ids
            silver_distinct_events = self.spark.sql(
                f"""
                SELECT COUNT(DISTINCT global_event_id) as distinct_count
                FROM silver.gdelt_events_detailed
                WHERE {time_filter} AND global_event_id IS NOT NULL
            """
            ).collect()[0]["distinct_count"]

            # Gold distinct event_ids
            gold_distinct_events = self.spark.sql(
                f"""
                SELECT COUNT(DISTINCT global_event_id) as distinct_count
                FROM gold_prod.gold_dashboard_master
                WHERE {time_filter} AND global_event_id IS NOT NULL
            """
            ).collect()[0]["distinct_count"]

            results = {
                "silver_distinct_events": silver_distinct_events,
                "gold_distinct_events": gold_distinct_events,
                "transformation_ratio": round(
                    (
                        (gold_distinct_events / silver_distinct_events * 100)
                        if silver_distinct_events > 0
                        else 0.0
                    ),
                    2,
                ),
            }

            logger.info(f"Silver Distinct Events: {silver_distinct_events:,}")
            logger.info(f"Gold Distinct Events: {gold_distinct_events:,}")
            logger.info(f"Transformation Ratio: {results['transformation_ratio']:.2f}%")

            self.audit_results["point_3"] = results
            return results

        except Exception as e:
            logger.error(f"Point 3 Audit Failed: {str(e)}")
            raise

    def audit_point_4_gold_to_postgres_accuracy(self) -> Dict[str, int]:
        """4차 감사: Gold → Postgres 데이터 이전 정확성 검증"""
        logger.info("Starting Point 4 Audit: Gold → Postgres Data Transfer Accuracy")

        try:
            time_filter = self.get_time_filter_condition()

            # Gold 총 레코드 수
            gold_count = self.spark.sql(
                f"""
                SELECT COUNT(*) as count
                FROM gold_prod.gold_dashboard_master
                WHERE {time_filter}
            """
            ).collect()[0]["count"]

            # Postgres 총 레코드 수
            postgres_df = (
                self.spark.read.format("jdbc")
                .option("url", os.getenv("POSTGRES_JDBC_URL"))
                .option(
                    "dbtable",
                    f"(SELECT COUNT(*) as count FROM gold_prod.gold_dashboard_master WHERE {time_filter}) as postgres_count",
                )
                .option("user", os.getenv("POSTGRES_USER"))
                .option("password", os.getenv("POSTGRES_PASSWORD"))
                .load()
            )
            postgres_count = postgres_df.collect()[0]["count"]

            results = {
                "gold_count": gold_count,
                "postgres_count": postgres_count,
                "transfer_accuracy": 100.0 if gold_count == postgres_count else 0.0,
            }

            logger.info(f"Gold Records: {gold_count:,}")
            logger.info(f"Postgres Records: {postgres_count:,}")
            logger.info(f"Transfer Accuracy: {results['transfer_accuracy']:.1f}%")

            # 100% 일치 검증 (필수)
            if gold_count != postgres_count:
                raise ValueError(
                    f"CRITICAL: Gold ({gold_count:,}) != Postgres ({postgres_count:,}) - Data Transfer Failed!"
                )

            self.audit_results["point_4"] = results
            return results

        except Exception as e:
            logger.error(f"Point 4 Audit Failed: {str(e)}")
            raise

    def run_full_audit(self) -> Dict[str, any]:
        """전체 4점 감사 실행"""
        start_time = time.time()
        logger.info("Starting 4-Point Data Audit System")
        logger.info(f"Time Window: Last {self.time_window_hours} hour(s)")

        try:
            # 각 포인트별 감사 실행
            self.audit_point_1_source_to_bronze()
            self.audit_point_2_bronze_to_silver_join_rate()
            self.audit_point_3_silver_to_gold_integrity()
            self.audit_point_4_gold_to_postgres_accuracy()

            # 감사 소요 시간 계산
            audit_duration = time.time() - start_time

            # 최종 결과 요약
            logger.info("=== 4-Point Data Audit Summary ===")
            logger.info(
                f"Point 1 - Bronze Total: {self.audit_results['point_1']['total_bronze']:,}"
            )
            logger.info(
                f"Point 2 - Join Success Rate: {self.audit_results['point_2']['join_success_rate']:.2f}%"
            )
            logger.info(
                f"Point 3 - Transformation Ratio: {self.audit_results['point_3']['transformation_ratio']:.2f}%"
            )
            logger.info(
                f"Point 4 - Transfer Accuracy: {self.audit_results['point_4']['transfer_accuracy']:.1f}%"
            )
            logger.info(f"Audit Duration: {audit_duration:.2f} seconds")
            logger.info("4-Point Data Audit PASSED!")

            # Prometheus 메트릭 전송
            if self.enable_metrics and self.metrics_exporter:
                time_window_label = f"{self.time_window_hours}h"
                self.metrics_exporter.export_metrics(
                    self.audit_results, time_window_label, audit_duration
                )

            return self.audit_results

        except Exception as e:
            audit_duration = time.time() - start_time
            logger.error(f"4-Point Data Audit FAILED: {str(e)}")

            # 실패 시에도 메트릭 전송 (audit_status = 0)
            if self.enable_metrics and self.metrics_exporter:
                time_window_label = f"{self.time_window_hours}h"
                failure_results = getattr(self, "audit_results", {})
                self.metrics_exporter.export_metrics(
                    failure_results, time_window_label, audit_duration
                )

            raise


def run_data_validation(time_window_hours: int = 1, enable_metrics: bool = True):
    """4점 감사 시스템 메인 실행 함수"""
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark = get_spark_session("4Point_Data_Audit", spark_master)

    try:
        audit_system = FourPointAuditSystem(spark, time_window_hours, enable_metrics)
        results = audit_system.run_full_audit()
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    run_data_validation()
