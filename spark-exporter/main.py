import os
import sys
import logging
import redis
import requests
from flask import Flask, Response
import threading
import time

# --- JMX 결과를 저장할 전역 변수 (인메모리 캐시) ---
jmx_metric_cache = {"driver": "", "executor": ""}


# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Flask 앱 초기화 ---
app = Flask(__name__)

# --- 환경 변수 읽기 및 검증 (Fail-Fast) ---
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

required_configs = {
    "SPARK_MASTER_URL": SPARK_MASTER_URL,
    "REDIS_HOST": REDIS_HOST,
    "REDIS_PORT": REDIS_PORT,
}

missing_configs = [key for key, value in required_configs.items() if not value]
if missing_configs:
    logging.critical(
        f"FATAL: Missing required environment variables: {', '.join(missing_configs)}"
    )
    logging.critical("Please add them to your .env file and restart the service.")
    sys.exit(1)


def add_label_to_jmx(jmx_text, source_name):
    """JMX 메트릭 텍스트의 모든 라인에 'jmx_source' 라벨을 추가한다."""
    processed_lines = []
    for line in jmx_text.strip().split("\n"):
        if line.startswith("#"):
            processed_lines.append(line)
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            processed_lines.append(line)
            continue
        metric_part, value_part = parts
        if "{" in metric_part and "}" in metric_part:
            metric_part = metric_part.replace("}", f', jmx_source="{source_name}"}}')
        else:
            metric_part = f'{metric_part}{{jmx_source="{source_name}"}}'
        processed_lines.append(f"{metric_part} {value_part}")
    return "\n".join(processed_lines) + "\n"


def scrape_jmx_in_background():
    """백그라운드 스레드에서 주기적으로 JMX 메트릭을 스크랩하여 캐시에 저장한다."""
    while True:
        try:
            driver_jmx_url = "http://spark-master:8090/metrics"
            jmx_driver_response = requests.get(driver_jmx_url, timeout=5)
            if jmx_driver_response.status_code == 200:
                processed_text = add_label_to_jmx(jmx_driver_response.text, "driver")
                jmx_metric_cache["driver"] = processed_text
        except requests.exceptions.RequestException as e:
            logging.warning(f"Background scrape failed for Driver JMX: {e}")

        try:
            executor_jmx_url = "http://spark-worker:8091/metrics"
            jmx_executor_response = requests.get(executor_jmx_url, timeout=5)
            if jmx_executor_response.status_code == 200:
                processed_text = add_label_to_jmx(
                    jmx_executor_response.text, "executor"
                )
                jmx_metric_cache["executor"] = processed_text
        except requests.exceptions.RequestException as e:
            logging.warning(f"Background scrape failed for Executor JMX: {e}")

        time.sleep(15)


@app.route("/metrics")
def respond_metrics():
    """
    Spark Master API와 Redis를 조합하여 활성 Spark 앱의 메트릭을 수집하고,
    백그라운드에서 수집된 JMX 메트릭과 함께 Prometheus 형식으로 노출한다.
    """
    metrics_report = ""
    try:
        r = redis.Redis(
            host=REDIS_HOST, port=int(REDIS_PORT), db=0, decode_responses=True
        )
        r.ping()

        master_api_url = f"http://{SPARK_MASTER_URL}/json/"
        response = requests.get(master_api_url, timeout=5)
        response.raise_for_status()
        active_apps = response.json().get("activeapps", [])
        metrics_report += f"spark_running_applications {len(active_apps)}\n"

        for app_data in active_apps:
            try:
                app_id = app_data.get("id")
                app_name = app_data.get("name")

                redis_key_pattern = f"spark_driver_ui:*:{app_id}"
                matched_keys = list(r.scan_iter(redis_key_pattern))
                if not matched_keys:
                    continue
                driver_ui_url = r.get(matched_keys[0])
                if not driver_ui_url:
                    continue

                driver_labels = f'app_id="{app_id}", app_name="{app_name}"'

                # --- Driver API 호출 (Jobs) ---
                jobs_api_url = f"{driver_ui_url}/api/v1/applications/{app_id}/jobs"
                jobs_response = requests.get(jobs_api_url, timeout=3)
                jobs_response.raise_for_status()
                jobs_data = jobs_response.json()
                if isinstance(jobs_data, list):
                    succeeded_jobs = len(
                        [j for j in jobs_data if j.get("status") == "SUCCEEDED"]
                    )
                    running_jobs = len(
                        [j for j in jobs_data if j.get("status") == "RUNNING"]
                    )
                    failed_jobs = len(
                        [j for j in jobs_data if j.get("status") == "FAILED"]
                    )
                else:
                    succeeded_jobs, running_jobs, failed_jobs = 0, 0, 0

                metrics_report += (
                    f"spark_driver_succeeded_jobs{{{driver_labels}}} {succeeded_jobs}\n"
                )
                metrics_report += (
                    f"spark_driver_running_jobs{{{driver_labels}}} {running_jobs}\n"
                )
                metrics_report += (
                    f"spark_driver_failed_jobs{{{driver_labels}}} {failed_jobs}\n"
                )

                # --- Driver API 호출 (Stages) ---
                stages_api_url = f"{driver_ui_url}/api/v1/applications/{app_id}/stages"
                stages_response = requests.get(stages_api_url, timeout=3)
                stages_response.raise_for_status()
                stages_data = stages_response.json()
                if isinstance(stages_data, list):
                    active_stages = len(
                        [s for s in stages_data if s.get("status") == "ACTIVE"]
                    )
                    completed_stages = len(
                        [s for s in stages_data if s.get("status") == "COMPLETE"]
                    )
                    failed_stages = len(
                        [s for s in stages_data if s.get("status") == "FAILED"]
                    )
                else:
                    active_stages, completed_stages, failed_stages = 0, 0, 0

                metrics_report += (
                    f"spark_driver_active_stages{{{driver_labels}}} {active_stages}\n"
                )
                metrics_report += f"spark_driver_completed_stages{{{driver_labels}}} {completed_stages}\n"
                metrics_report += (
                    f"spark_driver_failed_stages{{{driver_labels}}} {failed_stages}\n"
                )

                # --- Driver API 호출 (Executors) ---
                executors_api_url = (
                    f"{driver_ui_url}/api/v1/applications/{app_id}/executors"
                )
                exec_response = requests.get(executors_api_url, timeout=3)
                exec_response.raise_for_status()
                executors = exec_response.json()

                for executor in executors:
                    exec_id = executor.get("id")
                    active_tasks = executor.get("activeTasks", 0)
                    memory_used = executor.get("memoryUsed", 0)
                    total_shuffle_read = executor.get("totalShuffleRead", 0)
                    total_shuffle_write = executor.get("totalShuffleWrite", 0)
                    executor_labels = f'app_id="{app_id}", app_name="{app_name}", executor_id="{exec_id}"'
                    metrics_report += f"spark_executor_active_tasks{{{executor_labels}}} {active_tasks}\n"
                    metrics_report += f"spark_executor_memory_used_bytes{{{executor_labels}}} {memory_used}\n"
                    metrics_report += f"spark_executor_shuffle_read_bytes{{{executor_labels}}} {total_shuffle_read}\n"
                    metrics_report += f"spark_executor_shuffle_write_bytes{{{executor_labels}}} {total_shuffle_write}\n"

                logging.info(
                    f"Successfully collected REST API metrics for app {app_id}"
                )

            except Exception as e:
                app_id_for_log = app_data.get("id", "unknown")
                logging.error(
                    f"Failed to fetch REST API data for app {app_id_for_log}, skipping. Reason: {e}"
                )

        # --- 캐시된 JMX 메트릭 추가 ---
        metrics_report += jmx_metric_cache["driver"]
        metrics_report += jmx_metric_cache["executor"]

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        metrics_report += "spark_exporter_unknown_error 1\n"

    return Response(metrics_report, mimetype="text/plain")


if __name__ == "__main__":
    # --- 백그라운드 JMX 스크래핑 스레드 시작 ---
    jmx_scraper_thread = threading.Thread(target=scrape_jmx_in_background, daemon=True)
    jmx_scraper_thread.start()

    app.run(host="0.0.0.0", port=9191, debug=True)
