"""
Spark Structured Streaming – Traffic Speed Detection
=====================================================

One Kafka topic `radar_events` carries events from BOTH radars:

  { "plate": "AB-123-CD", "radar_id": "A", "ts_ms": 1711234567000 }
  { "plate": "AB-123-CD", "radar_id": "B", "ts_ms": 1711234599000 }

This job:
  1. Reads the stream and splits it into two logical streams: A and B.
  2. Applies withWatermark() on each side to handle late data.
  3. Performs a Stream–Stream JOIN: matches the same plate seen by A then B
     within a bounded time window.
  4. Computes average speed: DISTANCE_KM / ((ts_b - ts_a) / 3_600_000)
  5. Flags violations (speed > SPEED_LIMIT).
  6. Computes per-30-second windowed statistics (count, avg/max speed).
  7. Writes results to two output topics: `violations` and `traffic_stats`.

Key Structured Streaming concepts shown
----------------------------------------
  readStream              – consume an unbounded Kafka topic
  StructType / from_json  – parse JSON payloads with a schema
  withWatermark           – tell Spark how late events can arrive
  Stream–Stream JOIN      – join two streams on a key + time constraint
  window() + groupBy      – tumbling window aggregations
  outputMode("append")    – for joins and flat maps
  outputMode("update")    – for aggregations
  trigger(processingTime) – micro-batch cadence
  writeStream to Kafka    – publish results back to Kafka

Usage (from inside spark-master container)
-------------------------------------------
  spark-submit \
    --master spark://spark-master:7077 \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
    /opt/spark_jobs/job_traffic.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit,
    window, count, avg, max as spark_max,
    when, expr, round as spark_round,
    from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# ── Constants ──────────────────────────────────────────────────────────────────
DISTANCE_KM     = 3.0     # physical distance between Radar A and Radar B
SPEED_LIMIT     = 110.0   # km/h
KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC     = "radar_events"
CHECKPOINT_DIR  = "/tmp/checkpoints/traffic"

# ── Event schema ───────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("plate",    StringType(), False),
    StructField("radar_id", StringType(), False),
    StructField("ts_ms",    LongType(),   False),
])


def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HighwaySpeedDetection")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        # Fewer shuffle partitions for a local demo (default 200 is too many)
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
    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")

    events = read_radar_stream(spark)

    # ── 1. Split into two logical streams ─────────────────────────────────────
    # Both still read from the same physical stream; we just filter differently.

    stream_a = (
        events
        .filter(col("radar_id") == "A")
        # withWatermark: Spark will wait up to 15 min for late A-events
        # before it considers a time window "complete" and eligible for joining.
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

    # ── 2. Stream–Stream JOIN ──────────────────────────────────────────────────
    # Constraints:
    #   • Same plate
    #   • B must arrive AFTER A  (can't go backward)
    #   • B must arrive within 30 minutes of A  (no vehicle takes longer at ≥10 km/h)
    #
    # Spark keeps state (buffered rows) for both sides until the watermarks
    # allow it to safely close a window and emit (or discard) the join result.
    joined = stream_a.join(
        stream_b,
        expr("""
            plate_a = plate_b
            AND event_time_b > event_time_a
            AND event_time_b <= event_time_a + INTERVAL 30 MINUTES
        """),
        how="inner",
    )

    # ── 3. Compute speed and classify violation ────────────────────────────────
    results = (
        joined
        .withColumn("plate", col("plate_a"))
        # delta_t in hours: (ts_ms_b - ts_ms_a) / 3_600_000
        .withColumn(
            "delta_t_h",
            (col("ts_ms_b") - col("ts_ms_a")).cast("double") / 3_600_000.0,
        )
        # avg_speed = distance / time
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
        # Fine schedule (French scale, simplified)
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

    # ── 4. Sink A: all detections → `all_vehicles` topic ─────────────────────
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

    # ── 5. Sink B: violations only → `violations` topic ──────────────────────
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

    # ── 6. Sink C: 30-second tumbling window stats → `traffic_stats` topic ───
    # This uses a SEPARATE stream (re-read events) because window aggregations
    # require outputMode("update") which is incompatible with the join sink above.
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
