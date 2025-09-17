"""
파티셔닝 기반 Delta Lake Writer
- Spark SQL을 사용해 안정적으로 MERGE (UPSERT) 수행
- 소스 데이터의 중복을 사전 제거하고, 명시적 컬럼 매핑으로 스키마 불일치에 대응
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


def write_to_delta_lake(
    df: DataFrame, delta_path: str, table_name: str, partition_col: str, merge_key: str
):
    """DataFrame을 지정된 컬럼으로 파티셔닝하고, 지정된 키로 MERGE 수행"""
    logger.info(
        f"Saving {table_name} to {delta_path} using partition_col='{partition_col}' and merge_key='{merge_key}'"
    )

    if df.rdd.isEmpty():
        logger.warning(f"No records in DataFrame for {table_name}. Skipping write.")
        return

    spark = df.sparkSession

    # 1. 파티션 컬럼 추가
    df_with_partitions = (
        df.withColumn("year", F.year(F.col(partition_col)))
        .withColumn("month", F.month(F.col(partition_col)))
        .withColumn("day", F.dayofmonth(F.col(partition_col)))
        .withColumn("hour", F.coalesce(F.hour(F.col(partition_col)), F.lit(0)))
    )

    # 2. MERGE 안정성을 위해 소스 데이터의 중복을 제거
    # producer_timestamp가 있으면 그걸 기준으로, 없으면 partition_col(processed_at)을 기준으로 최신 데이터 선택
    timestamp_col = (
        "producer_timestamp" if "producer_timestamp" in df.columns else partition_col
    )

    window_spec = Window.partitionBy(merge_key).orderBy(F.col(timestamp_col).desc())

    df_clean = (
        df_with_partitions.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # 3. Delta 테이블 존재 여부 확인
    table_exists = False
    try:
        spark.read.format("delta").load(delta_path)
        table_exists = True
        logger.info(f"Delta table found at {delta_path}. Preparing for MERGE.")
    except AnalysisException:
        logger.info(f"Delta table not found at {delta_path}. Will create a new one.")

    # 4. MERGE 또는 최초 생성 수행
    if table_exists:
        # MERGE를 위해 임시 뷰 생성
        df_clean.createOrReplaceTempView("source_updates")

        # Target 테이블의 컬럼 목록을 가져옴
        target_columns = spark.read.format("delta").load(delta_path).columns

        # Source 테이블의 컬럼도 가져와서 교집합만 사용
        source_columns = df_clean.columns
        common_columns = [col for col in target_columns if col in source_columns]

        # 교집합 컬럼만으로 UPDATE/INSERT 구문을 생성
        update_mappings = ", ".join(
            [
                f"target.{col} = source.{col}"
                for col in common_columns
                if col != merge_key
            ]
        )
        insert_fields = ", ".join(common_columns)
        insert_values = ", ".join([f"source.{col}" for col in common_columns])

        logger.info(f"Using {len(common_columns)} common columns for MERGE")

        merge_sql = f"""
        MERGE INTO delta.`{delta_path}` AS target
        USING (SELECT {insert_fields} FROM source_updates) AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
          UPDATE SET {update_mappings}
        WHEN NOT MATCHED THEN
          INSERT ({insert_fields}) VALUES ({insert_values})
        """

        logger.info("Executing SQL MERGE...")
        spark.sql(merge_sql)
        logger.info(f"MERGE completed for {table_name}.")

    else:
        # 최초 생성 시에는 파티셔닝하여 저장
        logger.info(f"Creating new partitioned Delta table for {table_name}...")
        (
            df_clean.write.format("delta")
            .mode("overwrite")
            .partitionBy("year", "month", "day", "hour")
            .save(delta_path)
        )
        logger.info(f"New Delta table created for {table_name}.")
