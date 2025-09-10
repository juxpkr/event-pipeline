"""
Wikipedia Country Edit Counts Processor (Silver, 1-Minute Window)

This Spark batch job reads filtered Wikipedia edit events from Kafka,
aggregates the edit counts per country code over a 1-minute window,
and appends the results to a Silver Delta Lake table in MinIO.
"""
import os
import sys
from pathlib import Path
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Add project root to Python path
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, project_root)
from src.utils.spark_builder import get_spark_session

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WIKI_EDITS", "wiki_edits")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SILVER_TABLE_PATH = "s3a://warehouse/silver/wiki_country_edit_counts_1min"

def get_kafka_schema() -> StructType:
    """Defines the schema of the enriched JSON data from the producer."""
    return StructType([
        StructField("country_code", StringType(), True),
        StructField("event_timestamp", StringType(), True), # ISO 8601 string
    ])

def main():
    """Main function for the Spark batch job."""
    logger.info("🚀 Starting Spark job: Aggregate Wiki Edit Counts (1-Minute Window).")
    spark = get_spark_session("WikiCountryEditCounts1Min")

    try:
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if kafka_df.isEmpty():
            logger.warning("⚠️ No new messages in Kafka. Exiting.")
            return

        # Parse, transform, and add a proper timestamp column for windowing
        parsed_df = (
            kafka_df.select(from_json(col("value").cast("string"), get_kafka_schema()).alias("data"))
            .select("data.*")
            .withColumn("event_time", col("event_timestamp").cast(TimestampType()))
        )
        
        # Aggregate counts over a 1-minute tumbling window
        aggregated_df = (
            parsed_df.groupBy(
                window(col("event_time"), "1 minute").alias("time_window"),
                col("country_code")
            )
            .count()
            .withColumnRenamed("count", "edit_count")
        )
        
        # Flatten the window struct for easier querying
        final_df = aggregated_df.select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("country_code"),
            col("edit_count")
        ).filter(col("edit_count") > 0) # Only save windows with actual edits

        record_count = final_df.count()
        if record_count == 0:
            logger.warning("⚠️ No records remained after processing. Exiting.")
            return

        logger.info(f"💾 Saving {record_count} aggregated records to Delta table: {SILVER_TABLE_PATH}")
        (
            final_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(SILVER_TABLE_PATH)
        )
        
        logger.info("🎉 Successfully saved aggregated records.")
        logger.info("🔍 Sample of saved data:")
        final_df.orderBy("window_start", "country_code").show(truncate=False)

    except Exception as e:
        logger.error(f"❌ An error occurred during the Spark job: {e}", exc_info=True)
    finally:
        logger.info("✅ Spark session closed.")
        spark.stop()

if __name__ == "__main__":
    main()
