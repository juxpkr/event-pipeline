import os
import sys
import logging

# Airflow에서 실행할 때 프로젝트 루트를 경로에 추가
sys.path.append("/opt/airflow")

from src.utils.spark_builder import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """dbt Gold 테이블을 PostgreSQL로 Migration"""

    # Spark 세션 생성
    spark = get_spark_session(
        "Gold_To_PostgreSQL_Migration", "spark://spark-master:7077"
    )

    try:
        # dbt로 만든 gold table 읽기
        logger.info("📥 Reading Gold table from Hive Metastore...")
        # 여러 방법으로 시도
        try:
            gold_df = spark.sql("SELECT * FROM gold.gdelt_microbatch_country_analysis")
        except Exception as e:
            logger.info(f"첫 번째 방법 실패, 다른 방법 시도: {e}")
            try:
                gold_df = spark.table("gold.gdelt_microbatch_country_analysis")
            except Exception as e2:
                logger.info(f"두 번째 방법도 실패: {e2}")
                # 직접 Delta 파일 읽기 시도
                gold_df = spark.read.format("delta").load(
                    "s3a://gold/gdelt_microbatch_country_analysis"
                )

        record_count = gold_df.count()
        logger.info(f"📊 Found {record_count} records in Gold table")

        if record_count == 0:
            logger.warning("⚠️ No data found in Gold table!")
            return

        # PostgreSQL 연결 정보 (.env에서 가져옴)
        pg_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

        # PostgreSQL로 저장
        logger.info("💾 Writing to PostgreSQL...")
        (
            gold_df.write.format("jdbc")
            .option("url", pg_url)
            .option("dbtable", "gdelt_country_analysis")
            .option("user", os.getenv("POSTGRES_USER"))
            .option("password", os.getenv("POSTGRES_PASSWORD"))
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        logger.info(
            f"✅ Migration completed! {record_count} records written to PostgreSQL"
        )

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
