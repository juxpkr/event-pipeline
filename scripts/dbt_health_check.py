#!/usr/bin/env python3
"""
dbt 실행 전 환경 및 데이터 상태 진단 스크립트
- 테이블 존재 여부
- 데이터 존재 여부  
- 연결 상태
- 스키마 일치성
"""

import sys

sys.path.append("/app")

from pyspark.sql import SparkSession
from src.utils.spark_builder import get_spark_session
import logging


# 컬러 출력을 위한 ANSI 코드
class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_status(status, message):
    """상태에 따른 컬러 출력"""
    if status == "OK":
        print(f"{Colors.GREEN}✅ {message}{Colors.END}")
    elif status == "WARNING":
        print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")
    elif status == "ERROR":
        print(f"{Colors.RED}❌ {message}{Colors.END}")
    else:
        print(f"{Colors.BLUE}ℹ️  {message}{Colors.END}")


def check_spark_connection():
    """Spark 연결 상태 확인"""
    print(f"\n{Colors.BOLD}🔍 1. Spark 연결 상태 확인{Colors.END}")

    try:
        spark = get_spark_session("DBT Health Check", "spark://spark-master:7077")
        print_status("OK", "Spark 클러스터 연결 성공")
        return spark
    except Exception as e:
        print_status("ERROR", f"Spark 클러스터 연결 실패: {str(e)}")
        return None


def check_databases(spark):
    """데이터베이스 존재 확인"""
    print(f"\n{Colors.BOLD}🔍 2. 데이터베이스 존재 확인{Colors.END}")

    try:
        databases = spark.sql("SHOW DATABASES").collect()
        db_list = [row["namespace"] for row in databases]

        print_status("INFO", f"발견된 데이터베이스: {db_list}")

        # 필요한 데이터베이스 확인
        required_dbs = ["default", "silver", "gold"]
        for db in required_dbs:
            if db in db_list:
                print_status("OK", f"데이터베이스 '{db}' 존재함")
            else:
                print_status("WARNING", f"데이터베이스 '{db}' 없음 - 자동 생성됩니다")

        return True
    except Exception as e:
        print_status("ERROR", f"데이터베이스 조회 실패: {str(e)}")
        return False


def check_source_tables(spark):
    """소스 테이블 존재 및 데이터 확인"""
    print(f"\n{Colors.BOLD}🔍 3. 소스 테이블 상태 확인{Colors.END}")

    # 확인할 테이블 목록
    tables_to_check = [
        {"name": "default.gdelt_silver_events", "type": "silver"},
        {"name": "silver.gdelt_silver_events", "type": "silver"},
    ]

    found_tables = []

    for table_info in tables_to_check:
        table_name = table_info["name"]
        try:
            # 테이블 존재 확인
            spark.sql(f"DESCRIBE {table_name}")
            print_status("OK", f"테이블 '{table_name}' 존재함")

            # 데이터 개수 확인
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0][
                "cnt"
            ]
            if count > 0:
                print_status("OK", f"테이블 '{table_name}'에 {count:,}개 레코드 존재")
                found_tables.append(table_name)
            else:
                print_status("WARNING", f"테이블 '{table_name}'는 비어있음")

            # 스키마 확인
            schema = spark.sql(f"DESCRIBE {table_name}").collect()
            print_status("INFO", f"테이블 '{table_name}' 컬럼 수: {len(schema)}개")

        except Exception as e:
            print_status("ERROR", f"테이블 '{table_name}' 접근 실패: {str(e)}")

    return found_tables


def check_delta_files():
    """Delta 파일 존재 확인"""
    print(f"\n{Colors.BOLD}🔍 4. Delta 파일 상태 확인{Colors.END}")

    # 이 부분은 MinIO API나 직접 파일 시스템 접근이 필요
    # 현재는 Spark를 통한 간접 확인만 가능
    print_status("INFO", "Delta 파일은 테이블 접근을 통해 간접 확인됩니다")


def check_dbt_models():
    """dbt 모델 파일 확인"""
    print(f"\n{Colors.BOLD}🔍 5. dbt 모델 파일 확인{Colors.END}")

    import os

    model_paths = [
        "/app/transforms/models/staging/stg_gdelt_microbatch_events.sql",
        "/app/transforms/models/marts/gdelt_microbatch_country_analysis.sql",
    ]

    for model_path in model_paths:
        if os.path.exists(model_path):
            print_status("OK", f"모델 파일 존재: {os.path.basename(model_path)}")

            # SQL 파일 내용 간단히 체크
            with open(model_path, "r") as f:
                content = f.read()
                if (
                    "silver.gdelt_silver_events" in content
                    or "gdelt_silver_events" in content
                ):
                    print_status(
                        "OK", f"소스 테이블 참조 확인됨: {os.path.basename(model_path)}"
                    )
                else:
                    print_status(
                        "WARNING",
                        f"소스 테이블 참조 없음: {os.path.basename(model_path)}",
                    )
        else:
            print_status("ERROR", f"모델 파일 없음: {os.path.basename(model_path)}")


def generate_recommendations(found_tables):
    """문제점 기반 추천사항 생성"""
    print(f"\n{Colors.BOLD}💡 추천사항{Colors.END}")

    if not found_tables:
        print_status("ERROR", "소스 테이블이 없습니다!")
        print("  → 먼저 GDELT Silver Processor를 실행하세요:")
        print(
            "  → docker exec airflow-webserver spark-submit --master spark://spark-master:7077 /app/src/processing/batch/gdelt_silver_processor.py"
        )
        return False

    if len(found_tables) > 1:
        print_status("WARNING", "중복된 테이블이 발견되었습니다")
        print(f"  → 발견된 테이블: {found_tables}")
        print("  → dbt 모델에서 올바른 테이블을 참조하고 있는지 확인하세요")

    print_status("OK", "dbt run 실행 준비 완료!")
    return True


def main():
    """메인 진단 함수"""
    print(f"{Colors.BOLD}🏥 dbt 실행 전 건강 진단 시작{Colors.END}")
    print("=" * 60)

    # 1. Spark 연결 확인
    spark = check_spark_connection()
    if not spark:
        print_status("ERROR", "Spark 연결 실패로 진단 중단")
        return

    try:
        # 2. 데이터베이스 확인
        check_databases(spark)

        # 3. 소스 테이블 확인
        found_tables = check_source_tables(spark)

        # 4. Delta 파일 확인
        check_delta_files()

        # 5. dbt 모델 파일 확인
        check_dbt_models()

        # 6. 추천사항 생성
        print("\n" + "=" * 60)
        generate_recommendations(found_tables)

    finally:
        spark.stop()
        print(f"\n{Colors.BOLD}🏁 진단 완료{Colors.END}")


if __name__ == "__main__":
    main()
