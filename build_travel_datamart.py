from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    BooleanType,
    DateType,
)

# MinIO 접속 정보 설정
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = "http://minio:9000"

# Spark 세션 생성 (Delta Lake 및 S3A 설정 포함)
spark = (
    SparkSession.builder.appName("CryptoDataMartBuilder")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# 암호화폐 데이터 스키마 정의
crypto_ticker_schema = StructType(
    [
        StructField("type", StringType(), True),  # 타입 (ticker)
        StructField("code", StringType(), True),  # 마켓 코드 (KRW-BTC)
        StructField("opening_price", DoubleType(), True),  # 시가
        StructField("high_price", DoubleType(), True),  # 고가
        StructField("low_price", DoubleType(), True),  # 저가
        StructField("trade_price", DoubleType(), True),  # 현재가 (trade_price)
        StructField(
            "prev_closing_price", DoubleType(), True
        ),  # 전일 종가 (prev_closing_price)
        StructField("change", StringType(), True),  # 전일 대비 (CHANGE)
        StructField("signed_change_price", DoubleType(), True),  # 전일 대비 값
        StructField("change_price", DoubleType(), True),  # 부호 없는 전일 대비 값
        StructField("signed_change_rate", DoubleType(), True),  # 전일 대비 등락률
        StructField("change_rate", DoubleType(), True),  # 부호 없는 전일 대비 등락률
        StructField("trade_volume", DoubleType(), True),  # 최근 거래량
        StructField("acc_trade_volume", DoubleType(), True),  # 누적 거래량
        StructField("acc_trade_volume_24h", DoubleType(), True),  # 24시간 누적 거래량
        StructField("acc_trade_price", DoubleType(), True),  # 누적 거래대금
        StructField("acc_trade_price_24h", DoubleType(), True),  # 24시간 누적 거래대금
        StructField("trade_date", StringType(), True),  # 최근 거래 일자
        StructField("trade_time", StringType(), True),  # 최근 거래 시각
        StructField("trade_timestamp", LongType(), True),  # 체결 타임스탬프 (ms)
        StructField("ask_bid", StringType(), True),  # 매수/매도 구분
        StructField("acc_ask_volume", DoubleType(), True),  # 누적 매수량
        StructField("acc_bid_volume", DoubleType(), True),  # 누적 매도량
        StructField("highest_52_week_price", DoubleType(), True),  # 52주 최고가
        StructField("highest_52_week_date", StringType(), True),  # 52주 최고가 달성일
        StructField("lowest_52_week_price", DoubleType(), True),  # 52주 최저가
        StructField("lowest_52_week_date", StringType(), True),  # 52주 최저가 달성일
        StructField("trade_status", StringType(), True),
        StructField("market_state", StringType(), True),
        StructField("market_state_for_ios", StringType(), True),
        StructField("is_trading_suspended", BooleanType(), True),
        StructField("delisting_date", DateType(), True),
        StructField("market_warning", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("stream_type", StringType(), True),
    ]
)
# Kafka 소스에서 데이터 읽기
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "realtime-crypto-ticker")
    .option("startingOffsets", "earliest")
    .load()
)

# JSON 데이터를 컬럼으로 변환
raw_df = (
    kafka_df.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), crypto_ticker_schema).alias("data"))
    .select("data.*")
)


# -- 데이터 마트 로직 추가--
# 필요한 컬럼만 선택하고, 비즈니스 로직 추가
data_mart_df = raw_df.select(
    col("code").alias("market_code"),
    col("trade_price").alias("trade_price"),
    col("acc_trade_volume_24h").alias("volume_24h"),
    col("acc_trade_price_24h").alias("value_24h"),
    # 전일 대비 등락률을 퍼센트로 변환
    (col("change_rate") * 100).alias("change_rate_percent"),
)


# 엘리트 택배기사가 할 일을 정의하는 '배달 매뉴얼' 함수
def write_to_sinks(df, epoch_id):

    # 데이터가 비어있으면 아무것도 하지 않음
    if df.rdd.isEmpty():
        print(f"Batch {epoch_id}: No new data to process")
        return

    # MinIO(Delta Lake)에는 계속해서 Silver 데이터를 쌓아둔다)
    print(f"Batch {epoch_id}: Writing raw silver data to MinIO ... ")
    delta_path = "s3a://my-bucket/silver/crypto_ticker"
    df.write.format("delta").mode("append").save(delta_path)

    # PostgreSQL에는 '중간 재료' 테이블(stg_crypto_data)을 매번 새로 만든다.
    print(f"Batch {epoch_id}: Overwriting staging table in PostgreSQL...")
    postgres_url = "jdbc:postgresql://postgres:5432/airflow"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }
    # 중간 테이블(Staging Table) 이름
    table_name = "stg_crypto_data"
    df.write.jdbc(
        url=postgres_url,
        table=table_name,
        mode="overwrite",
        properties=postgres_properties,
    )
    print(f"Batch {epoch_id}: Successfully written to PostgreSQL staging table.")


# 스트리밍 쿼리 부분
query = (
    data_mart_df.writeStream.foreachBatch(write_to_sinks)
    .outputMode("update")
    .trigger(once=True)
    .option("checkpointLocation", "s3a://my-bucket/checkpoints/crypto_silver")
    .start()
)

# 쿼리가 끝날 때까지 기다림
query.awaitTermination()
