"""
silver_job.py — Maritime Navigation AI System
Bronze → Silver transformation:
  - Deduplicate records
  - Fill missing vessel info from best known values
  - Engineer features for ML training:
    sog_change, heading_change, time_delta, distance_nm
  - Add human-readable labels for vessel_type and status
  - Fine-grained grid bins (0.01°)

Run after bronze_job.py:
    docker compose exec spark-master \\
      /opt/spark/bin/spark-submit \\
        --packages io.delta:delta-core_2.12:2.4.0 \\
        src/processing/silver_job.py
"""
import sys
sys.path.insert(0, "/opt/spark/app/src/common")

from config import (
    SPARK_MASTER,
    DELTA_BRONZE_PATH, DELTA_SILVER_PATH,
)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, abs as spark_abs, sqrt,
    unix_timestamp, lit, when,
    round as spark_round, coalesce,
    first, last, row_number,
)

# Human-readable vessel type labels
VESSEL_TYPE_MAP = {
    "0": "Not Available",  "30": "Fishing",
    "31": "Towing",        "36": "Sailing",
    "37": "Pleasure Craft","50": "Pilot Vessel",
    "52": "Tug",           "60": "Passenger",
    "70": "Cargo",         "80": "Tanker",
    "35": "Military",      "51": "SAR",
    "90": "Other",
}

STATUS_MAP = {
    "0": "Underway/Engine", "1": "At Anchor",
    "2": "Not Under Command", "5": "Moored",
    "6": "Aground",         "7": "Fishing",
    "8": "Underway/Sailing",
}


def build_spark():
   return (
        SparkSession.builder
        .appName("Silver_AIS_Clean")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Increase shuffle partitions to reduce partition size
        .config("spark.sql.shuffle.partitions", "200")
        # Give more memory to each executor
        .config("spark.executor.memory", "3g")
        .config("spark.driver.memory",   "3g")
        # Allow spilling to disk when memory is full
        .config("spark.memory.fraction",          "0.8")
        .config("spark.memory.storageFraction",   "0.3")
        # Retry failed fetches
        .config("spark.shuffle.io.maxRetries",    "10")
        .config("spark.shuffle.io.retryWait",     "30s")
        # Timestamp fix
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .getOrCreate()
    )


def add_ml_features(df):
    """
    Engineer features needed for ML model training.
    All features computed per vessel ordered by time.
    """
    # Window: per vessel ordered by time
    w = Window.partitionBy("mmsi", "day").orderBy("base_datetime")

    return (
        df
        # ── Speed change ──────────────────────────────────────
        .withColumn("prev_sog",
                    lag("sog", 1).over(w))
        .withColumn("sog_change",
                    col("sog") - col("prev_sog"))

        # ── Heading change ─────────────────────────────────────
        .withColumn("prev_heading",
                    lag("heading", 1).over(w))
        .withColumn("raw_heading_change",
                    spark_abs(col("heading") - col("prev_heading")))
        # Normalise: heading 350→10 = 20°, not 340°
        .withColumn("heading_change",
                    when(col("raw_heading_change") > 180,
                         360 - col("raw_heading_change"))
                    .otherwise(col("raw_heading_change")))

        # ── Time delta ─────────────────────────────────────────
        .withColumn("prev_time",
                    lag("base_datetime", 1).over(w))
        .withColumn("time_delta_sec",
                    unix_timestamp("base_datetime") -
                    unix_timestamp("prev_time"))

        # ── Distance travelled ─────────────────────────────────
        .withColumn("prev_lat", lag("lat", 1).over(w))
        .withColumn("prev_lon", lag("lon", 1).over(w))
        # Haversine approximation in nautical miles
        .withColumn("distance_nm",
                    spark_round(
                        sqrt(
                            (col("lat") - col("prev_lat")) ** 2 +
                            (col("lon") - col("prev_lon")) ** 2
                        ) * 60.0, 4
                    ))

        # ── Fine grid bins ─────────────────────────────────────
        .withColumn("lat_bin_fine",
                    spark_round(col("lat"), 2))
        .withColumn("lon_bin_fine",
                    spark_round(col("lon"), 2))

        # ── Drop temp columns ──────────────────────────────────
        .drop("prev_sog", "prev_heading", "raw_heading_change",
              "prev_time", "prev_lat", "prev_lon")
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"📂  Reading Bronze Delta: {DELTA_BRONZE_PATH}")
    bronze = (
        spark.read.format("delta").load(DELTA_BRONZE_PATH)
        .filter(col("data_split").isin(["train", "test"]))
    )
    print(f"    Bronze rows (train+test): {bronze.count():,}")

    # ── Step 1: Deduplicate ────────────────────────────────────────────────────
    # Keep one record per vessel per minute
    deduped = bronze.dropDuplicates(["mmsi", "base_datetime"])
    print(f"    After dedup: {deduped.count():,}")

    # ── Step 2: Fill missing vessel info ──────────────────────────────────────
    # Some rows have vessel_name, others don't
    # Use the most complete record per vessel
    w_vessel = Window.partitionBy("mmsi")
    filled = (
        deduped
        .withColumn("vessel_name",
                    coalesce(col("vessel_name"),
                             first("vessel_name", True).over(w_vessel)))
        .withColumn("vessel_type",
                    coalesce(col("vessel_type"),
                             first("vessel_type", True).over(w_vessel)))
        .withColumn("imo",
                    coalesce(col("imo"),
                             first("imo", True).over(w_vessel)))
    )

    # ── Step 3: Add human-readable labels ─────────────────────────────────────
    type_expr = col("vessel_type")
    for code, label in VESSEL_TYPE_MAP.items():
        type_expr = when(col("vessel_type") == code, label).otherwise(
            type_expr
        )
    filled = filled.withColumn("vessel_type_label", type_expr)

    status_expr = col("status")
    for code, label in STATUS_MAP.items():
        status_expr = when(col("status") == code, label).otherwise(
            status_expr
        )
    filled = filled.withColumn("status_label", status_expr)

    # ── Step 4: Add ML features ────────────────────────────────────────────────
    silver = add_ml_features(filled)

    # ── Step 5: Write Silver Delta ─────────────────────────────────────────────
    print(f"\n💾  Writing to Silver Delta: {DELTA_SILVER_PATH}")
    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year", "month", "day")
        .save(DELTA_SILVER_PATH)
    )

    count = spark.read.format("delta").load(DELTA_SILVER_PATH).count()
    print(f"✅  Silver Delta has {count:,} records")
    print(f"\n    Next: run gold_job.py")
    spark.stop()


if __name__ == "__main__":
    main()
