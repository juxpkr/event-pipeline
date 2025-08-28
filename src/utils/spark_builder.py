import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def find_project_root() -> Path:
    # 현재 파일 위치에서부터 상위로 이동하며 .env 파일이 있는 프로젝트 루트를 찾는다.
    current_path = Path(__file__).resolve()

    # current_path가 루트 디렉토리('/')가 될 때까지 반복
    while current_path != current_path.parent:
        if (current_path / ".env").is_file():
            return current_path
        current_path = current_path.parent
    # 최상위까지 갔는데도 .env를 못 찾으면 에러 발생
    raise FileNotFoundError("Project root with .env file not found.")


def get_spark_session(app_name: str, master: str = None) -> SparkSession:
    # .env 파일과 환경 변수를 읽고, 프로젝트에서 표준으로 사용할 SparkSession을 생성하고 반환한다.

    # 동적 탐색 로직으로 .env 파일 경로 찾기
    try:
        project_root = find_project_root()
        load_dotenv(dotenv_path=(project_root / ".env"))
    except FileNotFoundError:
        print("Warning: .env file not found. Using default environment variables.")

    # .env 파일에서 버전 정보 읽기
    delta_spark_version = os.getenv("DELTA_SPARK_VERSION")
    hadoop_aws_version = os.getenv("HADOOP_AWS_VERSION")
    aws_sdk_version = os.getenv("AWS_SDK_VERSION")
    spark_kafka_version = os.getenv("SPARK_KAFKA_VERSION")
    postgres_jdbc_version = os.getenv("POSTGRESQL_JDBC_VERSION")
    scala_version = os.getenv("SCALA_VERSION", "2.12")

    packages = [
        f"io.delta:delta-core_{scala_version}:{delta_spark_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_kafka_version}",
        f"org.postgresql:postgresql:{postgres_jdbc_version}",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )

    # master 주소가 인자로 들어온 경우에만 설정 (Jupyter에서 클러스터 접속 시)
    if master:
        builder = builder.master(master)

    return builder.getOrCreate()
