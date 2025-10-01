import os
import requests
import redis
import time
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "http://spark-master:8080")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


def watch_spark_for_dbt():
    logging.info("Connecting to Redis...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    logging.info(f"Starting dbt watcher. Polling {SPARK_MASTER_URL} every 15 seconds.")

    while True:
        try:
            master_api_url = f"{SPARK_MASTER_URL}/json/"
            response = requests.get(master_api_url, timeout=5)
            response.raise_for_status()

            apps = response.json().get("activeapps", []) + response.json().get(
                "completedapps", []
            )

            for app in apps:
                app_name = app.get("name", "")
                if app_name.startswith("dbt-run-"):
                    app_id = app.get("id", "")
                    driver_ui_url = app.get("uiUrl")  # Spark 3.x+

                    redis_key = f"spark_driver_ui:{app_name}:{app_id}"
                    if not r.exists(redis_key):
                        r.set(redis_key, driver_ui_url, ex=300)
                        logging.info(f"DETECTED and REGISTERED dbt job: {redis_key}")

        except Exception as e:
            logging.error(f"Watcher loop failed: {e}")

        time.sleep(15)


if __name__ == "__main__":
    watch_spark_for_dbt()
