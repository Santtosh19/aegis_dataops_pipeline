# aegis/pipeline.py

import os
import sys

import hydra
from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_unixtime, hour, udf
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# --- SET THE PYTHON EXECUTABLE FOR SPARK WORKERS (CORRECTED) ---
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# ---------------------------------------------------


def safe_cast_to_double(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


safe_cast_udf = udf(safe_cast_to_double, DoubleType())


def run_pipeline(spark: SparkSession, cfg: DictConfig):
    print("Pipeline started. Reading raw data with an explicit schema...")
    schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("temperature", StringType(), True),
            StructField("metric_1", LongType(), True),
            StructField("status_code", StringType(), True),
        ]
    )
    raw_df = spark.read.csv(cfg.paths.raw_data, header=True, schema=schema)
    print(f"Successfully read {raw_df.count()} records.")
    transformed_df = (
        raw_df.withColumn("temperature_double", safe_cast_udf(col("temperature")))
        .filter(col("device_id").isNotNull())
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
    print("Pipeline finished successfully!")


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    spark = SparkSession.builder.appName(cfg.spark.app_name).master(cfg.spark.master).getOrCreate()
    run_pipeline(spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
