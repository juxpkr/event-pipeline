import os
import logging
from minio import Minio
from minio.error import S3Error

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_minio_client():
    """환경 변수를 사용하여 Minio 클라이언트를 초기화하고 반환합니다."""
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([endpoint, access_key, secret_key]):
        missing_vars = [
            var
            for var, val in {
                "MINIO_ENDPOINT": endpoint,
                "MINIO_ACCESS_KEY": access_key,
                "MINIO_SECRET_KEY": secret_key,
            }.items()
            if not val
        ]
        error_msg = f"❌ Missing MinIO environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # minio-py 클라이언트는 'http://' 같은 프로토콜 스킴을 제외한 엔드포인트를 기대합니다.
    if "://" in endpoint:
        endpoint = endpoint.split("://")[1]

    try:
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,  # 로컬 개발 환경에서는 보통 HTTP를 사용
        )
        return client
    except Exception as e:
        logger.error(f"❌ Minio 클라이언트 생성에 실패했습니다: {e}")
        raise


def ensure_bucket_exists(bucket_name: str):
    """
    MinIO에 버킷이 존재하는지 확인하고, 없으면 생성합니다.

    :param bucket_name: 확인할 버킷의 이름
    """
    logger.info(f"MinIO 버킷 '{bucket_name}'의 존재 여부를 확인합니다...")
    try:
        client = get_minio_client()
        found = client.bucket_exists(bucket_name)
        if not found:
            logger.info(f"버킷 '{bucket_name}'을(를) 찾을 수 없습니다. 새로 생성합니다...")
            client.make_bucket(bucket_name)
            logger.info(f"✅ 버킷 '{bucket_name}'이(가) 성공적으로 생성되었습니다.")
        else:
            logger.info(f"👍 버킷 '{bucket_name}'은(는) 이미 존재합니다.")
    except S3Error as e:
        logger.error(f"❌ 버킷 '{bucket_name}' 관련 MinIO S3 오류가 발생했습니다: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ ensure_bucket_exists 함수 실행 중 예기치 않은 오류가 발생했습니다: {e}")
        raise
