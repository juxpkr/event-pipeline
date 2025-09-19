import redis
import logging
import os


class RedisClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(RedisClient, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        # 이미 연결이 설정되었다면 다시 초기화하지 않음 (싱글턴 패턴)
        if hasattr(self, "connection") and self.connection:
            return

        try:
            self.connection = redis.Redis(
                host=os.getenv("REDIS_HOST"),
                port=int(os.getenv("REDIS_PORT")),
                db=0,
                decode_responses=True,  # 응답을 자동으로 UTF-8로 디코딩
            )
            self.connection.ping()  # 연결 테스트
            logging.info("Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            logging.error(f"Could not connect to Redis: {e}")
            self.connection = None

    def get_connection(self):
        return self.connection

    def register_driver_ui(self, spark_session, app_name):
        if not self.connection:
            logging.error("Redis connection not available. Skipping registration.")
            return

        try:
            app_id = spark_session.sparkContext.applicationId
            driver_ui_url = spark_session.sparkContext.uiWebUrl

            if driver_ui_url:
                redis_key = f"spark_driver_ui:{app_name}:{app_id}"
                # 1시간 뒤 자동 삭제
                self.connection.set(redis_key, driver_ui_url, ex=3600)
                logging.info(f"Registered Spark UI in Redis: {redis_key}")
            else:
                logging.warning("Spark UI URL not found.")
        except Exception as e:
            logging.error(f"Failed to register Spark UI to Redis: {e}")

    def unregister_driver_ui(self, spark_session):
        if not self.connection:
            logging.warning("Redis connection not available. Skipping unregistration.")
            return

        try:
            # spark_session에서 고유한 app_id를 가져오는 것이 핵심!
            app_id = spark_session.sparkContext.applicationId

            # app_id를 포함하는 모든 키를 찾는다 (보통 1개)
            redis_key_pattern = f"spark_driver_ui:*:{app_id}"
            matched_keys = list(self.connection.scan_iter(redis_key_pattern))

            if matched_keys:
                self.connection.delete(*matched_keys)
                logging.info(f"Unregistered Spark UI from Redis for app_id: {app_id}")
            else:
                logging.warning(
                    f"No key found in Redis to unregister for app_id: {app_id}"
                )

        except Exception as e:
            logging.error(f"Failed to unregister Spark UI from Redis: {e}")


# 다른 파일에서 이 클라이언트를 쉽게 가져다 쓸 수 있도록 인스턴스를 미리 생성
redis_client = RedisClient()
