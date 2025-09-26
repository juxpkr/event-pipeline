"""
GDELT Gold Layer to PostgreSQL Migration
dbt로 생성된 Gold Layer 테이블들을 PostgreSQL 데이터 마트로 이전
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import psycopg2

# 프로젝트 루트 경로 추가
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client
from pyspark.sql import SparkSession, DataFrame
from src.audit.lifecycle_updater import EventLifecycleUpdater

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
            # "gold_prod.gold_daily_detailed_events": {
            #     "postgres_table": "gold_daily_detailed_events",
            #     "postgres_schema": "gold",
            #     "description": "일별 상세 이벤트 집계 테이블 - incremental 모델이므로 migration 제외",
            # },
            "gold_prod.gold_near_realtime_summary": {
                "postgres_table": "gold_near_realtime_summary",
                "postgres_schema": "gold",
                "description": "준실시간 이벤트 요약 테이블",
            },
            "gold_prod.gold_superset_view": {
                "postgres_table": "gold_superset_view",
                "postgres_schema": "gold",
                "description": "Superset 대시보드용 뷰 테이블",
            },
            "gold_prod.gold_daily_actor_network": {
                "postgres_table": "gold_daily_actor_network",
                "postgres_schema": "gold",
                "description": "일별 국가간 연결망 분석 테이블",
            },
            "gold_prod.gold_chart_events_category": {
                "postgres_table": "gold_chart_events_category",
                "postgres_schema": "gold",
                "description": "차트용 카테고리별 이벤트 집계 테이블",
            },
            "gold_prod.gold_chart_weekday_event_ratio": {
                "postgres_table": "gold_chart_weekday_event_ratio",
                "postgres_schema": "gold",
                "description": "지난 2년 vs 이번 달 요일별 이벤트 발생률 비교 차트용 테이블",
            },
            "gold_prod.gold_chart_events_count_avgtone": {
                "postgres_table": "gold_chart_events_count_avgtone",
                "postgres_schema": "gold",
                "description": "전체 기간 이벤트 수 및 평균 톤 추이 차트용 테이블 (꺾은선 + 막대차트)",
            },
        }

    def _ensure_postgres_schema_exists(self, schema_name: str):
        """PostgreSQL에 지정된 스키마가 없으면 생성"""
        conn = None
        try:
            # Spark가 아닌 경량 psycopg2로 직접 연결
            conn = psycopg2.connect(
                dbname=self.postgres_config["database"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
            )
            conn.autocommit = True  # DDL은 auto-commit 모드로 실행
            with conn.cursor() as cur:
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
                logger.info(f"Executing pre-flight check: {create_schema_sql}")
                cur.execute(create_schema_sql)
            logger.info(f"Schema '{schema_name}' is ready in PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to create schema '{schema_name}' in PostgreSQL: {e}")
            raise  # 스키마 생성에 실패하면 파이프라인을 중단시켜야 함
        finally:
            if conn:
                conn.close()

    def prepare_postgres_schemas(self):
        """마이그레이션에 필요한 모든 PostgreSQL 스키마를 미리 준비한다."""
        logger.info("Starting pre-flight check: Preparing all PostgreSQL schemas...")
        # 마이그레이션할 테이블 목록에서 필요한 스키마 이름들을 중복 없이 추출
        schemas_to_create = {
            config["postgres_schema"] for config in self.migration_tables.values()
        }
        for schema in schemas_to_create:
            self._ensure_postgres_schema_exists(schema)
        logger.info("All PostgreSQL schemas are ready.")

    def _get_postgres_config(self) -> Dict[str, str]:
        """PostgreSQL 연결 설정 반환"""
        # Docker Swarm vs Compose 환경별 기본 호스트명 결정
        is_swarm = os.getenv("DOCKER_SWARM_MODE", "false").lower() == "true"
        default_postgres_host = "postgres" if is_swarm else "postgres"

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
            postgres_schema: PostgreSQL 스키마명

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
                .option("numPartitions", "4")  # 파티션 수 제한으로 커넥션 수 줄이기
                .option("batchsize", "50000")  # 배치 크기 늘려서 커넥션 재사용
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
        if gold_df is None:
            logger.error(f"Failed to read table '{gold_table}'. Skipping...")
            return False

        record_count = gold_df.count()
        if record_count == 0:
            logger.warning(
                f"Table '{gold_table}' is empty ({record_count} records), but proceeding with schema migration..."
            )
        else:
            logger.info(f"Found {record_count:,} records in '{gold_table}'")

        # 2. PostgreSQL에 저장
        success = self.write_to_postgres(
            gold_df,
            config["postgres_table"],
            config["description"],
            config["postgres_schema"],
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
                postgres_schema = config["postgres_schema"]
                full_table_name = f"{postgres_schema}.{postgres_table}"

                # 테이블 존재 및 레코드 수 확인
                count_query = (
                    f"(SELECT COUNT(*) as count FROM {full_table_name}) as count_table"
                )

                count_df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.postgres_config["url"])
                    .option("dbtable", count_query)
                    .option("user", self.postgres_config["user"])
                    .option("password", self.postgres_config["password"])
                    .option("driver", "org.postgresql.Driver")
                    .option("numPartitions", "1")  # 카운트만 하니까 파티션 1개면 충분
                    .load()
                )

                record_count = count_df.collect()[0]["count"]
                logger.info(
                    f"PostgreSQL table '{full_table_name}': {record_count:,} records"
                )

            logger.info("Migration verification completed successfully")
            return True

        except Exception as e:
            logger.error(f"Migration verification failed: {e}")
            return False


def main():
    """메인 실행 함수"""
    logger.info("Starting GDELT Gold to PostgreSQL Migration Pipeline...")

    # Spark 세션 생성
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark = get_spark_session("GDELT_Gold_To_PostgreSQL_Migration", spark_master)

    # Redis에 Spark Driver UI 정보 등록
    redis_client.register_driver_ui(spark, "GDELT Gold to PostgreSQL Migration")

    try:
        # 마이그레이터 인스턴스 생성
        migrator = GDELTGoldMigrator(spark)

        # PostgreSQL 스키마부터 모두 준비한다.
        migrator.prepare_postgres_schemas()

        # 모든 테이블 마이그레이션을 실행한다.
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

        # Lifecycle tracking: GOLD_COMPLETE 상태의 이벤트들을 POSTGRES_COMPLETE로 업데이트
        try:
            lifecycle_updater = EventLifecycleUpdater(spark)
            updated_count = lifecycle_updater.bulk_update_status(
                "GOLD_COMPLETE", "POSTGRES_COMPLETE"
            )
            logger.info(f"Updated {updated_count} events to POSTGRES_COMPLETE status")
        except Exception as e:
            logger.warning(f"Failed to update lifecycle status: {e}")

    except Exception as e:
        logger.error(f"Critical error in migration pipeline: {e}", exc_info=True)
        sys.exit(1)

    finally:
        try:
            redis_client.unregister_driver_ui(spark)
        except:
            pass
        spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    main()
