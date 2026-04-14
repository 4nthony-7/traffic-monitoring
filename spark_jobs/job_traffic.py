#!/usr/bin/env python3
__author__ = "Anthony Poher"
__date__ = "2026-04-07"

"""
Job Traffic: Spark Structured Streaming job to detect vehicles
----------------------------------------------------------------------------------
This script implements a Spark Structured Streaming job that :
  --> reads radar events from a Kafka topic : "radar_events",
  --> performs stream-stream joins to match radar A and B detections,
  --> computes average speed and flags violations
  --> writes results to three Kafka topics:
                            - 'all_vehicles'
                            - 'violations'
                            - 'traffic_stats' (30s windowed counts)
----------------------------------------------------------------------------------
Usage:
        --> docker exec spark-master /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
                /opt/spark_jobs/job_traffic.py
"""


import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, LongType)
from pyspark.sql.functions import (col, from_json, to_json, struct, lit,
                                window, count, avg, when, expr, round as spark_round)


# ── Constants ──────────────────────────────────────────────────────────────────
DISTANCE_KM     = 3.0
SPEED_LIMIT     = 110.0
KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC     = "radar_events"
CHECKPOINT_DIR  = "/tmp/checkpoints/traffic"
EVENT_SCHEMA = StructType([
    StructField("plate",    StringType(), False),
    StructField("radar_id", StringType(), False),
    StructField("ts_ms",    LongType(),   False),
])


# ── Functions ──────────────────────────────────────────────────────────────────
def create_session() -> SparkSession:
    """
    Create and configure the SparkSession for our streaming job.
     --> Kafka package : org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
     --> shuffle partitions : 4 (enough for our local demo)
    """
    return (
        SparkSession.builder
        .appName("HighwaySpeedDetection")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_radar_stream(spark: SparkSession):
    """
    Read the raw Kafka stream and parse the JSON payload.
    Returns a DataFrame with columns: plate, radar_id, ts_ms, event_time
                where event_time is a proper Timestamp derived from ts_ms.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select(
            col("e.plate"),
            col("e.radar_id"),
            col("e.ts_ms"),
            # Convert millisecond epoch to Timestamp for Spark's event-time engine
            (col("e.ts_ms") / 1000).cast("timestamp").alias("event_time"),
        )
        .filter(col("plate").isNotNull())
    )
    return parsed


def main():
    """
    Main functionnalities:
        --> withWatermark : Spark will wait up to 15 min for late events
        --> window : 30s tumbling windows for traffic stats
        --> trigger(processingTime) : micro-batch cadence of 5s for joins, 15s for stats
    """
    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")

    events = read_radar_stream(spark)

    # ──── Split into two logical streams ─────────────────────────────────────
    stream_a = (
        events
        .filter(col("radar_id") == "A")
        .withWatermark("event_time", "15 minutes")
        .select(
            col("plate").alias("plate_a"),
            col("ts_ms").alias("ts_ms_a"),
            col("event_time").alias("event_time_a"),
        )
    )

    stream_b = (
        events
        .filter(col("radar_id") == "B")
        .withWatermark("event_time", "15 minutes")
        .select(
            col("plate").alias("plate_b"),
            col("ts_ms").alias("ts_ms_b"),
            col("event_time").alias("event_time_b"),
        )
    )

    # ──── Stream–Stream JOIN ────────────────────────────────────────────────────
    # Constraints:
    #   - Same plate
    #   - B must arrive AFTER A
    #   - B must arrive within 30 minutes of A
    joined = stream_a.join(
        stream_b,
        expr("""
            plate_a = plate_b
            AND event_time_b > event_time_a
            AND event_time_b <= event_time_a + INTERVAL 30 MINUTES
        """),
        how="inner",
    )

    # ──── Compute speed and classify violation ────────────────────────────────
    results = (
        joined
        .withColumn("plate", col("plate_a"))
        # delta_t in hours: (ts_ms_b - ts_ms_a) / 3_600_000
        .withColumn(
            "delta_t_h",
            (col("ts_ms_b") - col("ts_ms_a")).cast("double") / 3_600_000.0,
        )
        # speed = distance / time
        .withColumn(
            "avg_speed_kmh",
            spark_round(lit(DISTANCE_KM) / col("delta_t_h"), 1),
        )
        .withColumn("violation", col("avg_speed_kmh") > SPEED_LIMIT)
        .withColumn(
            "excess_kmh",
            when(col("violation"),
                 spark_round(col("avg_speed_kmh") - SPEED_LIMIT, 1))
            .otherwise(lit(0.0)),
        )
        # Fine schedule
        .withColumn(
            "fine_eur",
            when(col("excess_kmh") > 50,  lit(1500))
            .when(col("excess_kmh") > 40,  lit(400))
            .when(col("excess_kmh") > 30,  lit(135))
            .when(col("excess_kmh") > 20,  lit(90))
            .when(col("excess_kmh") > 0,   lit(68))
            .otherwise(lit(0)),
        )
        # Keep only physically plausible rows
        .filter(col("delta_t_h") > 0)
    )

    # ──── Sink: 'all_vehicles' ─────────────────────────────────────────────────
    all_query = (
        results
        .select(
            col("plate").alias("key"),
            to_json(struct(
                "plate", "ts_ms_a", "ts_ms_b",
                "avg_speed_kmh", "violation", "excess_kmh", "fine_eur",
                col("event_time_a").cast("string").alias("detected_at"),
            )).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "all_vehicles")
        .option("checkpointLocation", CHECKPOINT_DIR + "/all_vehicles")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    time.sleep(3)

    # ──── Sink: 'violations' ───────────────────────────────────────────────────
    viol_query = (
        results
        .filter(col("violation"))
        .select(
            col("plate").alias("key"),
            to_json(struct(
                "plate", "ts_ms_a", "ts_ms_b",
                "avg_speed_kmh", "excess_kmh", "fine_eur",
                col("event_time_a").cast("string").alias("detected_at"),
            )).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "violations")
        .option("checkpointLocation", CHECKPOINT_DIR + "/violations")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    time.sleep(3)

    # ──── Sink: 'traffic stats' ───────────────────────────────────────────────────
    events2 = read_radar_stream(spark).filter(col("radar_id") == "A")

    stats_query = (
        events2
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "30 seconds"))
        .agg(
            count("*").alias("vehicles_at_a"),
            spark_round(avg(lit(0)), 0).alias("_placeholder"),  # aggregation req.
        )
        .select(
            to_json(struct(
                col("window.start").cast("string").alias("window_start"),
                col("window.end").cast("string").alias("window_end"),
                "vehicles_at_a",
            )).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "traffic_stats")
        .option("checkpointLocation", CHECKPOINT_DIR + "/stats")
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
