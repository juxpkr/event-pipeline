from pyspark.sql import functions as F

import sys

sys.path.append("/app")

from src.utils.spark_builder import get_spark_session

# Spark 세션 생성
spark = get_spark_session("GDELT_Migration_Daily_to_Hour", "spark://spark-master:7077")

# 마이그레이션할 테이블 목록
tables = [
    "s3a://warehouse/silver/gdelt_events",
    "s3a://warehouse/silver/gdelt_events_detailed",
]

for table_path in tables:
    table_name = table_path.split("/")[-1]
    print(f"{table_name} 마이그레이션 시작...")

    # 1. 기존에 '일별'로 파티셔닝된 테이블을 읽어온다.
    daily_partitioned_df = spark.read.format("delta").load(table_path)

    # 2. 파티셔닝에 필요한 'hour' 컬럼을 추가한다.
    df_with_hour = daily_partitioned_df.withColumn(
        "hour", F.hour(F.col("processed_time"))
    )

    # 3. 시간별 파티션으로 덮어쓰기
    print(f"{table_name} 시간별 파티셔닝 적용...")
    df_with_hour.write.format("delta").mode("overwrite").partitionBy(
        "year", "month", "day", "hour"
    ).save(table_path)

    print(f"{table_name} 마이그레이션 완료!")

print("모든 테이블 마이그레이션 완료!")
