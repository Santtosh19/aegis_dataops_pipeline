# aegis/pipeline.py

# --- Imports ---
import os
import shutil
import sys

import hydra
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_unixtime, hour, udf
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import DoubleType

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
    """Forcefully deletes output directories, ignoring errors if they are locked."""
    print("--- Cleaning up old output directories ---")
    for path_key in ["processed_data", "quarantined_data"]:
        dir_path = cfg.paths.get(path_key)
        if dir_path and os.path.exists(dir_path):
            print(f"Attempting to delete directory: {dir_path}")
            try:
                shutil.rmtree(dir_path)
                print(f"Successfully deleted {dir_path}")
            except OSError as e:
                # If we get a permission error, just print a warning and continue.
                print(f"WARNING: Could not delete {dir_path}. Reason: {e}. Continuing anyway.")


# --- THE NEW PYSPARK-NATIVE QUALITY GATEWAY ---
def validate_and_separate_data(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validates data using PySpark and separates it into clean and quarantined dataframes.
    """
    print("Applying data quality rules...")

    # Define all the "good" conditions at once
    valid_status_codes = ["200", "201", "400", "404", "500"]

    # We chain all the conditions for what makes a "clean" record
    clean_df = df.filter(
        col("device_id").isNotNull()
        & col("status_code").isin(valid_status_codes)
        &
        # Check that our safe cast does NOT produce a null
        safe_cast_udf(col("temperature")).isNotNull()
    )

    # The quarantined records are simply all records that are NOT in the clean set.
    # This is a much more reliable way to find them.
    quarantined_df = df.join(clean_df, on=df.columns, how="left_anti")

    return clean_df, quarantined_df


# --- Main Pipeline Logic ---
def run_pipeline(spark: SparkSession, cfg: DictConfig):
    """Main ETL logic function with built-in PySpark validation."""

    # --- Data Loading ---
    print("--- Loading Raw Data ---")
    raw_df = spark.read.csv(cfg.paths.raw_data, header=True, inferSchema=True)

    # --- Data Validation Step ---
    print("\n--- Phase 2: PySpark Data Quality Gateway ---")
    clean_df, quarantined_df = validate_and_separate_data(raw_df)

    print(f"Found {quarantined_df.count()} records to quarantine.")
    print(f"Proceeding with {clean_df.count()} valid records.")

    # Write quarantined data
    quarantined_df.write.mode("overwrite").parquet(cfg.paths.quarantined_data)

    # --- Data Transformation Step ---
    print("\n--- Phase 1: Data Transformation Step ---")

    # NOTE: We now run the safe_cast_udf AGAIN on the clean data.
    # This is because the original check was only to identify the bad rows.
    transformed_df = (
        clean_df.withColumn("temperature_double", safe_cast_udf(col("temperature")))
        .filter(col("device_id").isNotNull())  # This filter is now redundant but safe to keep
        .withColumn("event_hour", hour(from_unixtime(col("timestamp"))))
    )

    aggregated_df = transformed_df.groupBy("device_id", "event_hour").agg(
        avg("temperature_double").alias("avg_temperature"),
        _sum("metric_1").alias("total_metric_1"),
        count("*").alias("record_count"),
    )

    print("Data aggregated. Writing to processed directory...")
    aggregated_df.write.mode("overwrite").partitionBy("event_hour").parquet(
        cfg.paths.processed_data
    )

    print("\nPipeline finished successfully!")
    print("Check the 'data/processed' and 'data/quarantined' folders.")


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    # ...
    spark = (
        SparkSession.builder.appName(cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    run_pipeline(spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
