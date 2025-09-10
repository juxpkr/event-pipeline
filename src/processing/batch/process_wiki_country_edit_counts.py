"""
Wikipedia Per-Document Edit Counts Processor (Silver)

Reads filtered Wikipedia edit events from Kafka, aggregates the edit counts
per specific article title, and appends the results to a Silver Delta Lake table.
"""
import os
import sys
from pathlib import Path
import logging
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Add project root to Python path
project_root = os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, project_root)
from src.utils.spark_builder import get_spark_session

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WIKI_EDITS", "wiki_edits")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SILVER_TABLE_PATH = "s3a://warehouse/silver/wiki_per_document_edit_counts" # New path

def get_kafka_schema() -> StructType:
    """Defines the schema of the enriched JSON data from the producer."""
    return StructType([
        StructField("country_code", StringType(), True),
        StructField("english_name", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("domain", StringType(), True),
    ])

def main():
    """Main function for the Spark batch job."""
    logger.info("🚀 Starting Spark job: Aggregate Wiki Edit Counts Per Document.")
    spark = get_spark_session("WikiPerDocumentEditCounts")

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

        parsed_df = kafka_df.select(from_json(col("value").cast("string"), get_kafka_schema()).alias("data")).select("data.*")
        
        # Aggregate counts per document title
        aggregated_df = (
            parsed_df.groupBy("country_code", "english_name", "page_title", "domain")
            .count()
            .withColumnRenamed("count", "edit_count")
        )
        
        final_df = aggregated_df.withColumn("processed_at", current_timestamp())

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
        final_df.show(truncate=False)

    except Exception as e:
        logger.error(f"❌ An error occurred during the Spark job: {e}", exc_info=True)
    finally:
        logger.info("✅ Spark session closed.")
        spark.stop()

if __name__ == "__main__":
    main()