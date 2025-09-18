"""
GDELT Gold Layer to PostgreSQL Migration
dbt로 생성된 Gold Layer 테이블들을 PostgreSQL 데이터 마트로 이전
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from pyspark.sql import SparkSession, DataFrame

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class GDELTGoldMigrator:
    """GDELT Gold Layer 데이터를 PostgreSQL로 마이그레이션하는 클래스"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.postgres_config = self._get_postgres_config()

        # 마이그레이션할 테이블 정의
        self.migration_tables = {
            "gold.gold_1st_global_overview": {
                "postgres_table": "gold_1st_global_overview",
                "postgres_schema": "gold",
                "description": "전세계 레벨 - 국가별 & 일일 요약",
            },
            "gold.gold_2nd_country_events": {
                "postgres_table": "gold_country_events",
                "postgres_schema": "gold",
                "description": "국가간 이벤트 분석",
            },
            "gold.gold_4th_daily_detail_summary": {
                "postgres_table": "gold_daily_detail_summary",
                "postgres_schema": "gold",
                "description": "이벤트 상세 - 일일 요약",
            },
            "gold.gdelt_microbatch_country_analysis": {
                "postgres_table": "gold_microbatch_country_analysis",
                "postgres_schema": "gold",
                "description": "GDELT 마이크로배치 데이터의 국가별/이벤트 타입별 분석",
            },
        }

    def _get_postgres_config(self) -> Dict[str, str]:
        """PostgreSQL 연결 설정 반환"""
        # Docker Swarm vs Compose 환경별 기본 호스트명 결정
        is_swarm = os.getenv("DOCKER_SWARM_MODE", "false").lower() == "true"
        default_postgres_host = "geoevent_postgres" if is_swarm else "postgres"

        postgres_host = os.getenv("POSTGRES_HOST", default_postgres_host)
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        postgres_db = os.getenv("POSTGRES_DB", "airflow")

        return {
            "host": postgres_host,
            "port": postgres_port,
            "database": postgres_db,
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
            "url": f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
        }

    def read_gold_table(self, table_name: str) -> Optional[DataFrame]:
        """
        Gold Layer 테이블을 읽어 DataFrame으로 반환

        Args:
            table_name: 읽을 테이블명 (예: silver.stg_seed_mapping)

        Returns:
            DataFrame 또는 None (실패 시)
        """
        logger.info(f"Reading Gold table '{table_name}'...")

        try:
            # Hive Metastore를 통해 테이블 읽기
            gold_df = self.spark.table(table_name)
            record_count = gold_df.count()
            logger.info(
                f"Successfully read {record_count:,} records from '{table_name}'"
            )
            return gold_df

        except Exception as e:
            logger.error(f"Failed to read table '{table_name}': {e}")
            return None

    def write_to_postgres(
        self, df: DataFrame, postgres_table: str, description: str, postgres_schema: str
    ) -> bool:
        """
        DataFrame을 PostgreSQL 테이블에 저장

        Args:
            df: 저장할 DataFrame
            postgres_table: PostgreSQL 테이블명
            description: 테이블 설명

        Returns:
            성공 여부 (bool)
        """
        try:
            record_count = df.count()
            logger.info(
                f"Writing {record_count:,} records to PostgreSQL table '{postgres_table}'..."
            )
            logger.info(f"Description: {description}")

            full_table_name = f"{postgres_schema}.{postgres_table}"

            # PostgreSQL에 저장
            (
                df.write.format("jdbc")
                .option("url", self.postgres_config["url"])
                .option("dbtable", full_table_name)
                .option("user", self.postgres_config["user"])
                .option("password", self.postgres_config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("overwrite")  # 기존 데이터 덮어쓰기
                .save()
            )

            logger.info(f"Successfully migrated to PostgreSQL table '{postgres_table}'")
            return True

        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL table '{postgres_table}': {e}")
            return False

    def migrate_single_table(self, gold_table: str, config: Dict[str, str]) -> bool:
        """
        단일 테이블 마이그레이션

        Args:
            gold_table: Gold Layer 테이블명
            config: 마이그레이션 설정

        Returns:
            성공 여부 (bool)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Migrating: {gold_table} → {config['postgres_table']}")
        logger.info(f"{'='*60}")

        # 1. Gold 테이블 읽기
        gold_df = self.read_gold_table(gold_table)
        if gold_df is None or gold_df.count() == 0:
            logger.warning(f"No data found in '{gold_table}'. Skipping...")
            return False

        # 2. PostgreSQL에 저장
        success = self.write_to_postgres(
            gold_df, config["postgres_table"], config["description"], config["postgres_schema"]
        )

        return success

    def migrate_all_tables(self) -> Dict[str, bool]:
        """
        모든 Gold 테이블을 PostgreSQL로 마이그레이션

        Returns:
            테이블별 마이그레이션 결과 딕셔너리
        """
        logger.info("Starting GDELT Gold to PostgreSQL Migration...")
        logger.info(f"Target PostgreSQL: {self.postgres_config['url']}")
        logger.info(f"Tables to migrate: {len(self.migration_tables)}")

        results = {}
        successful_migrations = 0

        for gold_table, config in self.migration_tables.items():
            try:
                success = self.migrate_single_table(gold_table, config)
                results[gold_table] = success

                if success:
                    successful_migrations += 1

            except Exception as e:
                logger.error(f"Critical error migrating '{gold_table}': {e}")
                results[gold_table] = False

        # 최종 결과 요약
        logger.info(f"\n{'='*60}")
        logger.info("MIGRATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total tables: {len(self.migration_tables)}")
        logger.info(f"Successful: {successful_migrations}")
        logger.info(f"Failed: {len(self.migration_tables) - successful_migrations}")

        for table, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"{status}: {table}")

        return results

    def verify_migration(self) -> bool:
        """
        마이그레이션 결과 검증
        PostgreSQL 테이블들이 제대로 생성되었는지 확인
        """
        logger.info("\nVerifying migration results...")

        try:
            # PostgreSQL 연결 테스트용 간단한 쿼리
            for config in self.migration_tables.values():
                postgres_table = config["postgres_table"]

                # 테이블 존재 및 레코드 수 확인
                count_query = (
                    f"(SELECT COUNT(*) as count FROM {postgres_table}) as count_table"
                )

                count_df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.postgres_config["url"])
                    .option("dbtable", count_query)
                    .option("user", self.postgres_config["user"])
                    .option("password", self.postgres_config["password"])
                    .option("driver", "org.postgresql.Driver")
                    .load()
                )

                record_count = count_df.collect()[0]["count"]
                logger.info(
                    f"PostgreSQL table '{postgres_table}': {record_count:,} records"
                )

            logger.info("Migration verification completed successfully")
            return True

        except Exception as e:
            logger.error(f"Migration verification failed: {e}")
            return False


def main():
    """메인 실행 함수"""
    logger.info("Starting GDELT Gold to PostgreSQL Migration Pipeline...")

    # Spark 세션 생성 (환경에 따라 동적 설정)
    is_swarm = os.getenv("DOCKER_SWARM_MODE", "false").lower() == "true"
    default_spark_master = (
        "spark://geoevent_spark-master:7077"
        if is_swarm
        else "spark://spark-master:7077"
    )
    spark_master = os.getenv("SPARK_MASTER_URL", default_spark_master)
    spark = get_spark_session("GDELT_Gold_To_PostgreSQL_Migration", spark_master)

    try:
        # 마이그레이터 인스턴스 생성
        migrator = GDELTGoldMigrator(spark)

        # 모든 테이블 마이그레이션 실행
        results = migrator.migrate_all_tables()

        # 마이그레이션 검증
        verification_success = migrator.verify_migration()

        # 실패한 테이블이 있으면 에러 코드로 종료
        failed_count = sum(1 for success in results.values() if not success)
        if failed_count > 0:
            logger.error(f"Migration completed with {failed_count} failures")
            sys.exit(1)

        if not verification_success:
            logger.error("Migration verification failed")
            sys.exit(1)

        logger.info("All migrations completed successfully")

    except Exception as e:
        logger.error(f"Critical error in migration pipeline: {e}", exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    main()
