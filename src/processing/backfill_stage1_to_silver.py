"""
GDELT 백필 1단계: Producer → Consumer → Silver
특정 날짜 범위의 데이터를 Raw부터 Silver Layer까지 처리
"""

import os
import sys
import subprocess
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트를 Python path에 추가
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_command(command, description, cwd=None):
    """
    외부 명령어를 실행하고 결과를 로깅
    """
    logger.info(f"Starting: {description}")
    logger.info(f"Command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd or project_root
        )

        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout.strip()}")

        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.error(f"STDOUT: {e.stdout}")
        if e.stderr:
            logger.error(f"STDERR: {e.stderr}")
        return False


def validate_date_format(date_str):
    """날짜 형식 검증 (YYYY-MM-DD HH:MM:SS)"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def main():
    parser = argparse.ArgumentParser(description="GDELT 백필 1단계: Producer → Consumer → Silver")
    parser.add_argument(
        "--start-date",
        required=True,
        help="백필 시작 날짜 (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="백필 종료 날짜 (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--skip-producer",
        action="store_true",
        help="Producer 단계 건너뛰기 (이미 Kafka에 데이터가 있는 경우)",
    )
    parser.add_argument(
        "--skip-consumer",
        action="store_true",
        help="Consumer 단계 건너뛰기 (이미 Bronze에 데이터가 있는 경우)",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Silver 단계 건너뛰기 (Bronze까지만 처리)",
    )

    args = parser.parse_args()

    # 날짜 형식 검증
    if not validate_date_format(args.start_date):
        logger.error(f"Invalid start date format: {args.start_date}. Use 'YYYY-MM-DD HH:MM:SS'")
        sys.exit(1)

    if not validate_date_format(args.end_date):
        logger.error(f"Invalid end date format: {args.end_date}. Use 'YYYY-MM-DD HH:MM:SS'")
        sys.exit(1)

    start_time = datetime.strptime(args.start_date, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(args.end_date, "%Y-%m-%d %H:%M:%S")

    if start_time >= end_time:
        logger.error("Start date must be before end date")
        sys.exit(1)

    logger.info("="*60)
    logger.info("GDELT 백필 1단계 시작")
    logger.info(f"기간: {args.start_date} ~ {args.end_date}")
    logger.info("="*60)

    success_steps = []
    failed_steps = []

    # Step 1: Producer (Raw 데이터 수집 → Kafka)
    if not args.skip_producer:
        logger.info("\n🚀 Step 1: GDELT Producer (Raw → Kafka)")
        producer_cmd = [
            "python",
            "src/processing/gdelt_producer_backfill.py",
            "--start-date", args.start_date,
            "--end-date", args.end_date,
            "--logical-date", args.start_date
        ]

        if run_command(producer_cmd, "GDELT Producer Backfill"):
            success_steps.append("Producer")
        else:
            failed_steps.append("Producer")
            logger.error("Producer failed. Stopping pipeline.")
            sys.exit(1)
    else:
        logger.info("⏭️ Step 1: GDELT Producer (SKIPPED)")
        success_steps.append("Producer (Skipped)")

    # Step 2: Consumer (Kafka → Bronze)
    if not args.skip_consumer:
        logger.info("\n📥 Step 2: GDELT Consumer (Kafka → Bronze)")
        consumer_cmd = [
            "python",
            "src/processing/gdelt_bronze_consumer_backfill.py",
            "--start-time", args.start_date,
            "--end-time", args.end_date
        ]

        if run_command(consumer_cmd, "GDELT Consumer Backfill"):
            success_steps.append("Consumer")
        else:
            failed_steps.append("Consumer")
            logger.error("Consumer failed. Stopping pipeline.")
            sys.exit(1)
    else:
        logger.info("⏭️ Step 2: GDELT Consumer (SKIPPED)")
        success_steps.append("Consumer (Skipped)")

    # Step 3: Silver Processor (Bronze → Silver)
    if not args.skip_silver:
        logger.info("\n✨ Step 3: GDELT Silver Processor (Bronze → Silver)")
        silver_cmd = [
            "python",
            "src/processing/gdelt_silver_processor.py",
            args.start_date,
            args.end_date
        ]

        if run_command(silver_cmd, "GDELT Silver Processor"):
            success_steps.append("Silver Processor")
        else:
            failed_steps.append("Silver Processor")
            logger.error("Silver Processor failed.")
            sys.exit(1)
    else:
        logger.info("⏭️ Step 3: GDELT Silver Processor (SKIPPED)")
        success_steps.append("Silver Processor (Skipped)")

    # 최종 결과 요약
    logger.info("\n" + "="*60)
    logger.info("GDELT 백필 1단계 완료!")
    logger.info("="*60)

    if success_steps:
        logger.info("✅ 성공한 단계:")
        for step in success_steps:
            logger.info(f"  - {step}")

    if failed_steps:
        logger.error("❌ 실패한 단계:")
        for step in failed_steps:
            logger.error(f"  - {step}")
        sys.exit(1)

    logger.info(f"\n🎉 백필 완료: {args.start_date} ~ {args.end_date}")
    logger.info("Silver Layer까지 데이터 처리가 완료되었습니다.")
    logger.info("\n다음 단계: Gold Layer 처리를 위해 dbt 실행")


if __name__ == "__main__":
    main()