import os
import sys
import logging
from pathlib import Path

# sys.path.append 대신, 이 파일의 위치를 기준으로 프로젝트 루트를 찾아서 경로에 추가
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def read_gold_table(
    spark: SparkSession, table_name: str, table_path: str
) -> DataFrame | None:
    """
    Gold Layer 테이블을 읽어 DataFrame으로 반환한다.
    Metastore에서 먼저 찾고, 실패 시 S3 경로에서 직접 읽는다.
    """
    logger.info(f"📥 Reading Gold table '{table_name}' from Hive Metastore...")
    try:
        # 1. Hive Metastore를 통해 테이블 읽기
        gold_df = spark.table(table_name)
        logger.info(f"✅ Successfully read from Metastore.")
        return gold_df
    except Exception:
        logger.warning(
            f"⚠️ Could not find table '{table_name}' in Metastore. "
            f"Attempting to read directly from Delta path: {table_path}"
        )
        try:
            # 2. S3 경로에서 직접 Delta 파일 읽기
            gold_df = spark.read.format("delta").load(table_path)
            logger.info(f"✅ Successfully read from S3 path.")
            return gold_df
        except Exception as e:
            logger.error(
                f"❌ Failed to read Gold data from both Metastore and S3 path.",
                exc_info=True,
            )
            return None


def write_to_postgres(df: DataFrame, dbtable: str):
    # DataFrame을 PostgreSQL 테이블에 덮어쓴다.
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    # Gold 데이터의 최종 목적지는 Airflow DB로 지정
    postgres_db = os.getenv("POSTGRES_DB", "airflow")
    pg_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

    logger.info(
        f"💾 Writing {df.count()} records to PostgreSQL table '{dbtable}' at {pg_url}..."
    )

    (
        df.write.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", dbtable)
        .option("user", os.getenv("POSTGRES_USER", "airflow"))
        .option("password", os.getenv("POSTGRES_PASSWORD", "airflow"))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    logger.info(f"✅ Migration completed successfully to table '{dbtable}'.")


def main():
    # 메인 실행 함수
    logger.info("🚀 Starting Gold to PostgreSQL Migration...")
    spark = get_spark_session(
        "Gold_To_PostgreSQL_Migration", "spark://spark-master:7077"
    )

    try:
        # 1. Gold 테이블 읽기
        gold_table_name = "gold.gdelt_microbatch_country_analysis"
        gold_table_path = "s3a://warehouse/gold/gdelt_microbatch_country_analysis"
        gold_df = read_gold_table(spark, gold_table_name, gold_table_path)

        if gold_df is None or gold_df.rdd.isEmpty():
            logger.warning("⚠️ No data found in Gold table. Exiting gracefully.")
            return

        # 2. PostgreSQL에 쓰기
        write_to_postgres(gold_df, "gdelt_country_analysis")

    except Exception as e:
        logger.error(
            f"❌ A critical error occurred in the migration pipeline: {e}",
            exc_info=True,
        )
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("✅ Spark session closed")


if __name__ == "__main__":
    main()
