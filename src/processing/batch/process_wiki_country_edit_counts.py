"""
Wikipedia Country Edit Counts Processor (Silver, 1-Minute Window)

This Spark batch job reads filtered Wikipedia edit events from Kafka,
aggregates the edit counts per country code over a 1-minute window,
and appends the results to a single, non-partitioned Silver Delta Lake table.
It uses Redis to manage the watermark for incremental processing.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, window, max as _max, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Add project root to Python path
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, project_root)
from src.utils.spark_builder import get_spark_session
from src.utils.redis_client import redis_client  # Import the Redis client instance

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WIKI_EDITS", "wiki_edits")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SILVER_TABLE_PATH = "s3a://warehouse/silver/wiki_country_edit_counts_1min"
REDIS_WATERMARK_KEY = "watermark:wiki_country_edit_counts_1min"


def get_kafka_schema() -> StructType:
    """Defines the schema of the enriched JSON data from the producer."""
    return StructType(
        [
            StructField("country_code", StringType(), True),
            StructField("event_timestamp", StringType(), True),  # ISO 8601 string
        ]
    )


def get_watermark_from_redis(redis_conn):
    """Fetches the last processed timestamp from Redis."""
    watermark = redis_conn.get(REDIS_WATERMARK_KEY)
    if watermark:
        logger.info(f"Found watermark in Redis: {watermark}")
        # Convert ISO 8601 string from Redis back to a datetime object
        return datetime.fromisoformat(watermark)
    logger.info("No watermark found in Redis. Assuming first run.")
    return None


def main():
    """Main function for the Spark batch job."""
    logger.info("🚀 Starting Spark job: Aggregate Wiki Edit Counts (1-Minute Window).")
    spark = get_spark_session("WikiCountryEditCounts1Min")
    redis_conn = redis_client.get_connection()

    if not redis_conn:
        logger.error("❌ Could not establish connection to Redis. Aborting job.")
        return

    try:
        # --- Determine the starting point for processing from Redis ---
        latest_processed_time = get_watermark_from_redis(redis_conn)

        # --- Read from Kafka ---
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")  # Read all, then filter
            .option("endingOffsets", "latest")
            .load()
        )

        if kafka_df.isEmpty():
            logger.warning(f"⚠️ No messages in Kafka topic '{KAFKA_TOPIC}'. Exiting.")
            return

        # --- Parse and Filter Data ---
        parsed_df = (
            kafka_df.select(
                from_json(col("value").cast("string"), get_kafka_schema()).alias("data")
            )
            .select("data.*")
            .withColumn("event_time", to_timestamp(col("event_timestamp")))
        )

        if latest_processed_time:
            logger.info(f"Filtering events with event_time > {latest_processed_time}")
            parsed_df = parsed_df.filter(col("event_time") > latest_processed_time)

        # --- Aggregate Data ---
        aggregated_df = (
            parsed_df.groupBy(
                window(col("event_time"), "1 minute").alias("time_window"),
                col("country_code"),
            )
            .count()
            .withColumnRenamed("count", "edit_count")
        )

        # --- Final Transformation ---
        final_df = aggregated_df.select(
            col("time_window.start").alias("window_time"),
            col("time_window.end").alias("window_end"),
            col("country_code"),
            col("edit_count"),
        ).filter(col("edit_count") > 0)

        record_count = final_df.count()
        if record_count == 0:
            logger.warning(
                "⚠️ No new records to process after filtering and aggregation. Exiting."
            )
            return

        # --- Save to Delta Lake ---
        logger.info(
            f"💾 Saving {record_count} new aggregated records to Delta table: {SILVER_TABLE_PATH}"
        )
        (
            final_df.coalesce(1)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(SILVER_TABLE_PATH)
        )

        logger.info("🎉 Successfully saved aggregated records.")
        logger.info("🔍 Sample of newly saved data:")
        final_df.orderBy("window_time", "country_code").show(truncate=False)

        # --- Update Watermark in Redis ---
        # Find the latest 'window_end' in the processed data
        new_watermark_row = final_df.select(_max("window_end")).first()
        if new_watermark_row and new_watermark_row[0]:
            new_watermark = new_watermark_row[0]
            # Store in ISO 8601 format for consistency
            redis_conn.set(REDIS_WATERMARK_KEY, new_watermark.isoformat())
            logger.info(
                f"✅ Successfully updated watermark in Redis to: {new_watermark.isoformat()}"
            )

    except Exception as e:
        logger.error(f"❌ An error occurred during the Spark job: {e}", exc_info=True)
    finally:
        logger.info("✅ Spark session closed.")
        spark.stop()


if __name__ == "__main__":
    main()
