import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, master: str = None) -> SparkSession:
    """
    프로젝트 표준 SparkSession을 생성하고 반환한다.
    - Docker 환경에 최적화 (환경 변수 사용)
    - S3 (MinIO), Hive Metastore, Delta Lake를 기본으로 지원한다.
    """
    scala_version = os.getenv("SCALA_VERSION", "2.12")
    delta_spark_version = os.getenv("DELTA_SPARK_VERSION", "2.4.0")
    hadoop_aws_version = os.getenv("HADOOP_AWS_VERSION", "3.3.4")
    aws_sdk_version = os.getenv("AWS_SDK_VERSION", "1.12.367")

    packages = (
        f"io.delta:delta-core_{scala_version}:{delta_spark_version},"
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},"
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}"
    )
    builder = (
        SparkSession.builder.appName(app_name)
        # --- Maven에서 Delta 패키지 직접 다운로드
        .config("spark.jars.packages", packages)
        # Spark 자원 독점 방지 설정
        .config("spark.cores.max", "2")
        # --- S3 (MinIO) 접속 설정 ---
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- Delta Lake 연동 설정 ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- Hive Metastore 연동 설정 ---
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.uris",
            os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083"),
        )
        .enableHiveSupport()
    )

    # master 주소가 인자로 들어온 경우(Airflow/클러스터 모드)에만 설정
    if master:
        builder = builder.master(master)

    return builder.getOrCreate()
