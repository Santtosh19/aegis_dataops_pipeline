# aegis/pipeline.py
import json  # <-- Import json
import logging  # <-- Import logging
import os
import shutil
import sys
import time  # <-- Import time

import hydra
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_unixtime, hour, udf
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import DoubleType

# --- Setup a structured logger ---
# We get a logger for this specific module
log = logging.getLogger(__name__)

# --- Environment and UDF Setup ---
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def safe_cast_to_double(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


safe_cast_udf = udf(safe_cast_to_double, DoubleType())


def cleanup_output_dirs(cfg: DictConfig):
    log.info("--- Cleaning up old output directories ---")
    for path_key in ["processed_data", "quarantined_data", "run_summary_path"]:
        dir_path = cfg.paths.get(path_key)
        # For summary, we delete the file, not the dir
        if path_key == "run_summary_path" and os.path.exists(dir_path):
            os.remove(dir_path)
            continue
        if dir_path and os.path.exists(dir_path):
            log.info(f"Deleting directory: {dir_path}")
            try:
                shutil.rmtree(dir_path)
                log.info(f"Successfully deleted {dir_path}")
            except OSError as e:
                log.warning(f"Could not delete {dir_path}. Reason: {e}. Continuing anyway.")


def validate_and_separate_data(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    log.info("Applying data quality rules...")
    valid_status_codes = ["200", "201", "400", "404", "500"]
    clean_df = df.filter(
        col("device_id").isNotNull()
        & col("status_code").isin(valid_status_codes)
        & safe_cast_udf(col("temperature")).isNotNull()
    )
    quarantined_df = df.join(clean_df, on=df.columns, how="left_anti")
    return clean_df, quarantined_df


def run_pipeline(spark: SparkSession, cfg: DictConfig):
    start_time = time.time()
    log.info("--- Loading Raw Data ---")
    raw_df = spark.read.csv(cfg.paths.raw_data, header=True, inferSchema=True)

    log.info("\n--- Phase 2: PySpark Data Quality Gateway ---")
    clean_df, quarantined_df = validate_and_separate_data(raw_df)

    quarantined_count = quarantined_df.count()
    clean_count = clean_df.count()
    log.info(f"Found {quarantined_count} records to quarantine.")
    log.info(f"Proceeding with {clean_count} valid records.")

    quarantined_df.write.mode("overwrite").parquet(cfg.paths.quarantined_data)

    log.info("\n--- Phase 1: Data Transformation Step ---")
    transformed_df = (
        clean_df.withColumn("temperature_double", safe_cast_udf(col("temperature")))
        .filter(col("device_id").isNotNull())
        .withColumn("event_hour", hour(from_unixtime(col("timestamp"))))
    )

    aggregated_df = transformed_df.groupBy("device_id", "event_hour").agg(
        avg("temperature_double").alias("avg_temperature"),
        _sum("metric_1").alias("total_metric_1"),
        count("*").alias("record_count"),
    )

    log.info("Data aggregated. Writing to processed directory...")
    aggregated_df.write.mode("overwrite").partitionBy("event_hour").parquet(
        cfg.paths.processed_data
    )

    # --- NEW: Create a run summary artifact ---
    end_time = time.time()
    summary = {
        "run_time_seconds": round(end_time - start_time, 2),
        "total_records_read": clean_count + quarantined_count,
        "valid_records_processed": clean_count,
        "quarantined_records": quarantined_count,
        "run_status": "Success",
    }
    summary_path = cfg.paths.get("run_summary_path", "run_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=4)

    log.info(f"Pipeline finished successfully! Summary written to {summary_path}")


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    # This sets up the basic configuration for the logger.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    summary_path = cfg.paths.get("run_summary_path", "run_summary.json")
    cleanup_output_dirs(cfg)

    spark = (
        SparkSession.builder.appName(cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    try:
        run_pipeline(spark, cfg)
    except Exception as e:
        log.error("!!! PIPELINE FAILED !!!", exc_info=True)
        # Create a failure summary if the pipeline crashes
        summary = {"run_status": "Failure", "error_message": str(e)}
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=4)
        raise e  # Re-raise the exception so the program still fails
    finally:
        spark.stop()
        log.info("Spark session stopped.")


if __name__ == "__main__":
    main()
