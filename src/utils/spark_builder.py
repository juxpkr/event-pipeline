import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, master: str = None) -> SparkSession:
    """
    프로젝트 표준 SparkSession을 생성하고 반환한다.
    - Docker 환경에 최적화 (환경 변수 사용)
    - S3 (MinIO), Hive Metastore, Delta Lake를 기본으로 지원한다.
    """
    builder = (
        SparkSession.builder.appName(app_name)
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
        .config("spark.jars", f"/opt/spark/jars/delta-core_2.12-{os.getenv('DELTA_VERSION', '2.4.0')}.jar,/opt/spark/jars/delta-storage-{os.getenv('DELTA_VERSION', '2.4.0')}.jar,/opt/spark/jars/hadoop-aws-{os.getenv('HADOOP_AWS_VERSION', '3.3.4')}.jar,/opt/spark/jars/aws-java-sdk-bundle-{os.getenv('AWS_SDK_VERSION', '1.12.262')}.jar")
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
