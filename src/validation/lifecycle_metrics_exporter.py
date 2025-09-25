"""
Lifecycle Metrics Exporter
Event Lifecycle 감사 결과를 Prometheus로 전송
"""

import os
import time
import json
import requests
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class LifecycleMetricsExporter:
    """Event Lifecycle 감사 메트릭을 Prometheus Pushgateway로 전송"""

    def __init__(self, pushgateway_url: str = None, job_name: str = "lifecycle_audit"):
        self.pushgateway_url = pushgateway_url or os.getenv(
            "PROMETHEUS_PUSHGATEWAY_URL", "http://pushgateway:9091"
        )
        self.job_name = job_name

    def export_lifecycle_metrics(self, audit_results: Dict):
        """Lifecycle 감사 결과를 Prometheus 메트릭으로 전송"""
        try:
            metrics_payload = self._build_lifecycle_metrics(audit_results)

            # Debug: payload 로깅
            logger.info(
                f"Sending metrics to: {self.pushgateway_url}/metrics/job/{self.job_name}"
            )
            logger.info(f"Metrics payload (first 500 chars): {metrics_payload[:500]}")

            response = requests.post(
                f"{self.pushgateway_url}/metrics/job/{self.job_name}",
                data=metrics_payload,
                headers={"Content-Type": "text/plain"},
            )

            # Debug: 응답 상태 로깅
            logger.info(f"Response status: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Response content: {response.text}")

            response.raise_for_status()

            logger.info(f"Lifecycle metrics exported to {self.pushgateway_url}")

        except Exception as e:
            logger.error(f"Failed to export lifecycle metrics: {str(e)}")

    def _build_lifecycle_metrics(self, audit_results: Dict) -> str:
        """Prometheus 형식의 lifecycle 메트릭 생성"""
        lines = []

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

        # === 1. 데이터 무결성 & 품질 메트릭 ===
        join_yield = audit_results.get("join_yield", {})
        add_metric(
            "gdelt_pipeline_events",
            join_yield.get("waiting_events", 0),
            labels={"status": "waiting"},
            help_text="Number of events by status in pipeline",
        )
        add_metric(
            "gdelt_pipeline_events",
            join_yield.get("joined_events", 0),
            labels={"status": "joined"},
        )
        add_metric(
            "gdelt_pipeline_events",
            join_yield.get("expired_events", 0),
            labels={"status": "expired"},
        )
        add_metric(
            "gdelt_join_yield_percentage",
            join_yield.get("join_yield", 0),
            help_text="Join success rate percentage",
        )

        sync = audit_results.get("gold_postgres_sync", {})
        add_metric(
            "gdelt_gold_postgres_sync_accuracy",
            sync.get("sync_accuracy", 0),
            help_text="Gold to Postgres synchronization accuracy (percentage)",
        )
        add_metric(
            "gdelt_layer_records",
            sync.get("gold_count", 0),
            labels={"layer": "gold"},
            help_text="Number of records by layer",
        )
        add_metric(
            "gdelt_layer_records",
            sync.get("postgres_count", 0),
            labels={"layer": "postgres"},
        )

        # === 전체 파이프라인 상태 ===
        overall_health = 1 if audit_results.get("overall_health", False) else 0
        add_metric(
            "gdelt_pipeline_health",
            overall_health,
            help_text="Overall pipeline health status (1=HEALTHY, 0=UNHEALTHY)",
        )

        audit_duration = audit_results.get("audit_duration", 0)
        add_metric(
            "gdelt_audit_duration_seconds",
            audit_duration,
            help_text="Duration of lifecycle audit in seconds",
        )

        # === 2. 파이프라인 속도 메트릭 ===
        durations = audit_results.get("stage_durations", {})
        add_metric(
            "gdelt_pipeline_duration_hours",
            durations.get("avg_e2e_duration_hours", 0),
            labels={"stage": "e2e"},
            help_text="Average pipeline duration by stage in hours",
        )
        add_metric(
            "gdelt_pipeline_duration_hours",
            durations.get("avg_silver_duration_hours", 0),
            labels={"stage": "silver"},
        )
        add_metric(
            "gdelt_pipeline_duration_hours",
            durations.get("avg_gold_duration_hours", 0),
            labels={"stage": "gold"},
        )
        add_metric(
            "gdelt_pipeline_duration_hours",
            durations.get("avg_postgres_duration_hours", 0),
            labels={"stage": "postgres"},
        )

        # === 실시간 현황 메트릭 ===
        current_time = time.time()
        add_metric(
            "gdelt_audit_last_run_timestamp",
            current_time,
            help_text="Timestamp of last audit execution",
        )

        return "\n".join(lines) + "\n"


def export_producer_collection_metrics(collection_stats: Dict):
    """Producer 수집 통계를 Prometheus로 전송"""
    try:
        exporter = LifecycleMetricsExporter(job_name="gdelt_producer")

        lines = []
        current_time = time.time()

        # 데이터 타입별 수집 레코드 수
        for data_type, stats in collection_stats.items():
            if isinstance(stats, dict) and "record_count" in stats:
                lines.append(
                    f"# HELP gdelt_collection_records_{data_type} Number of records collected for {data_type}"
                )
                lines.append(f"# TYPE gdelt_collection_records_{data_type} gauge")
                lines.append(
                    f'gdelt_collection_records_{data_type}{{data_type="{data_type}"}} {stats["record_count"]}'
                )

                lines.append(
                    f"# HELP gdelt_collection_urls_{data_type} Number of URLs processed for {data_type}"
                )
                lines.append(f"# TYPE gdelt_collection_urls_{data_type} gauge")
                lines.append(
                    f'gdelt_collection_urls_{data_type}{{data_type="{data_type}"}} {stats.get("url_count", 0)}'
                )

        # 전체 수집 통계
        total_records = sum(
            stats.get("record_count", 0)
            for stats in collection_stats.values()
            if isinstance(stats, dict)
        )
        lines.append(
            f"# HELP gdelt_collection_total_records Total number of records collected"
        )
        lines.append(f"# TYPE gdelt_collection_total_records gauge")
        lines.append(f"gdelt_collection_total_records {total_records}")

        lines.append(f"# HELP gdelt_collection_timestamp Timestamp of last collection")
        lines.append(f"# TYPE gdelt_collection_timestamp gauge")
        lines.append(f"gdelt_collection_timestamp {current_time}")

        metrics_payload = "\n".join(lines) + "\n"

        response = requests.post(
            f"{exporter.pushgateway_url}/metrics/job/{exporter.job_name}",
            data=metrics_payload,
            headers={"Content-Type": "text/plain"},
        )
        response.raise_for_status()

        logger.info(f"Producer collection metrics exported to Prometheus")
        return True

    except Exception as e:
        logger.error(f"Failed to export producer metrics: {e}")
        return False


def export_lifecycle_audit_metrics(audit_results: Dict):
    """Lifecycle 감사 결과 메트릭 전송 (독립 실행용)"""
    try:
        exporter = LifecycleMetricsExporter()
        exporter.export_lifecycle_metrics(audit_results)
        return True
    except Exception as e:
        logger.error(f"Failed to export metrics: {e}")
        return False
