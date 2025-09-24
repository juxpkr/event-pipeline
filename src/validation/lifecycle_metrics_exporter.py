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

            response = requests.post(
                f"{self.pushgateway_url}/metrics/job/{self.job_name}",
                data=metrics_payload,
                headers={"Content-Type": "text/plain"},
            )
            response.raise_for_status()

            logger.info(f"Lifecycle metrics exported to {self.pushgateway_url}")

        except Exception as e:
            logger.error(f"Failed to export lifecycle metrics: {str(e)}")

    def _build_lifecycle_metrics(self, audit_results: Dict) -> str:
        """Prometheus 형식의 lifecycle 메트릭 생성"""
        lines = []
        timestamp = int(time.time())

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

            lines.append(f"{name}{label_str} {value} {timestamp}")

        # === 수집 정확성 메트릭 ===
        collection = audit_results.get("collection_accuracy", {})
        add_metric(
            "gdelt_collection_rate",
            collection.get("collection_rate", 0),
            help_text="GDELT data collection rate percentage",
        )
        add_metric(
            "gdelt_expected_batches",
            collection.get("expected_batches", 0),
            help_text="Expected number of GDELT batches",
        )
        add_metric(
            "gdelt_tracked_events",
            collection.get("tracked_events", 0),
            help_text="Number of events tracked in lifecycle",
        )

        # === 조인 성공률 메트릭 ===
        join_yield = audit_results.get("join_yield", {})
        add_metric(
            "gdelt_join_yield",
            join_yield.get("join_yield", 0),
            help_text="Event join success rate for mature events (percentage)",
        )
        add_metric(
            "gdelt_mature_events_total",
            join_yield.get("total_mature_events", 0),
            help_text="Total mature events (12-24h old)",
        )
        add_metric(
            "gdelt_joined_events",
            join_yield.get("joined_events", 0),
            help_text="Number of successfully joined events",
        )
        add_metric(
            "gdelt_waiting_events",
            join_yield.get("waiting_events", 0),
            help_text="Number of events still waiting for join",
        )

        # === 데이터 유실 탐지 메트릭 ===
        data_loss = audit_results.get("data_loss_detection", {})
        add_metric(
            "gdelt_suspicious_events",
            data_loss.get("suspicious_events", 0),
            help_text="Number of suspicious events (waiting >24h)",
        )
        add_metric(
            "gdelt_auto_expired_events",
            data_loss.get("auto_expired", 0),
            help_text="Number of auto-expired old events",
        )

        # === Gold-Postgres 동기화 메트릭 ===
        sync = audit_results.get("gold_postgres_sync", {})
        add_metric(
            "gdelt_gold_postgres_sync",
            sync.get("sync_accuracy", 0),
            help_text="Gold to Postgres synchronization accuracy (percentage)",
        )
        add_metric(
            "gdelt_gold_records",
            sync.get("gold_count", 0),
            help_text="Number of records in Gold layer",
        )
        add_metric(
            "gdelt_postgres_records",
            sync.get("postgres_count", 0),
            help_text="Number of records in Postgres",
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

        # === 실시간 현황 메트릭 ===
        current_time = time.time()
        add_metric(
            "gdelt_audit_last_run_timestamp",
            current_time,
            help_text="Timestamp of last audit execution",
        )

        return "\n".join(lines) + "\n"


def export_lifecycle_audit_metrics(audit_results: Dict):
    """Lifecycle 감사 결과 메트릭 전송 (독립 실행용)"""
    try:
        exporter = LifecycleMetricsExporter()
        exporter.export_lifecycle_metrics(audit_results)
        return True
    except Exception as e:
        logger.error(f"Failed to export metrics: {e}")
        return False
