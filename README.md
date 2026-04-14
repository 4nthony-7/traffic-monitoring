# Highway Speed Control — Spark Structured Streaming

A **Apache Spark Structured Streaming** demo applied to automated highway speed enforcement: 
two radars 3 km apart detect the same vehicles; Spark joins the two streams in real time, computes 
the average speed, and triggers an alert for any violation above the legal limit.

> Built to demonstrate key Spark Structured Streaming concepts in a real-time stream processing context.

---

## Tech Stack

| Component | Image / technology | Role |
|-----------|--------------------|------|
| Kafka (KRaft) | `apache/kafka:latest` | Message bus |
| Spark master + 2 workers | `spark:python3` (Docker Official) | Stream processing engine |
| Radar producers | `python:3.11-slim` | Simulate radars A and B |
| Dashboard | `python:3.11-slim` + Streamlit | Real-time visualisation |

All images are **open source** and licensed under Apache 2.0.

---

## Architecture
```
Radar A (km 0)                                   Radar B (km 5)
    │  {plate, radar_id:"A", ts_ms}                   │  {plate, radar_id:"B", ts_ms}
    │                                                 │
    └─────────────────────┐   ┌───────────────────────┘
                          ▼   ▼
                Kafka topic : radar_events
                            │
                    ┌───────▼────────┐
                    │   Spark job    │
                    │                │
                    │ filter A / B   │
                    │ withWatermark  │
                    │ Stream JOIN    │
                    │ speed = D / Δt │
                    │ window(30s)    │
                    └───────┬────────┘
                            |
                    Kafka output topics
                            |
                ┌───────────┼──────────┐
                ▼           ▼          ▼
          all_vehicles  violations  traffic_stats
                └───────────┴──────────┘
                            │
                  Streamlit Dashboard
```

---

## Spark Structured Streaming concepts illustrated

| Concept | Function / location |
|---------|---------------------|
| **Reading the Kafka stream** — connect to a Kafka topic as an infinite streaming DataFrame | `readStream` |
| **Parsing Kafka events** — deserialise the JSON payload against an explicit schema | `EVENT_SCHEMA` + `from_json` |
| **Late-event handling** — tolerate delayed events before closing a state window | `withWatermark` |
| **Fault tolerance** — warm restart with no data loss or duplicates | `CHECKPOINT_DIR` |
| **Plate matchmaking** — join two infinite streams with a bounded time constraint | `Stream–Stream JOIN` |
| **Speed & fine computation** — derived columns and cascading business rules | `withColumn` + `when / otherwise` |
| **Windowing** — time-based aggregations over 30-second tumbling windows | `window()` + `groupBy` |
| **Writing back to Kafka** — publish results to three output topics | `writeStream` |
| **Micro-batching** — controlled processing cadence for each sink | `trigger(processingTime=...)` |
| **Output modes** — `append` for joins (final result), `update` for aggregations | `outputMode("append")` / `outputMode("update")` |
| **Multiple concurrent queries** — three independent sinks running in parallel | `all_query`, `viol_query`, `stats_query` |

---

## Prerequisites

- **Docker Desktop** ≥ 24 running (green icon in the system tray)
- **Docker Compose** ≥ 2.20 (bundled with Docker Desktop)
- **4 GB of RAM** minimum allocated to Docker Desktop (`Settings → Resources → Memory`)
- Available ports: `7077`, `8080`, `8501`, `9094`

> **Windows**: use PowerShell or Git Bash from the project folder.

---

## Configurable parameters

| Parameter | File | Default value |
|-----------|------|---------------|
| Distance A → B | `job_traffic.py` + `app.py` | 3 km |
| Speed limit | `job_traffic.py` + `app.py` | 110 km/h |
| Late-event tolerance (watermark) | `job_traffic.py` | 15 min |
| Join window | `job_traffic.py` | 30 min |
| Statistics window | `job_traffic.py` | 30 s |
| Radar emission interval | `docker-compose.yml` | 600 ms |

---

## Quick start

### 1 — Check the file structure
```
traffic-monitoring/
├── docker-compose.yml
├── spark/
│   ├── Dockerfile
│   └── start-spark.sh       ← must use LF line endings (not CRLF)
├── producer/
│   ├── Dockerfile
│   └── producer.py
├── dashboard/
│   ├── Dockerfile
│   ├── app.py
│   └── style.css
├── spark_jobs/
│   └── job_traffic.py
└── scripts/
    └── submit_job.sh
```

> ⚠️ **Windows only** — `start-spark.sh` must be encoded with **LF** line endings.
> In VS Code, check the indicator in the bottom-right corner: it should read `LF`.
> If you see `CRLF`, click it and select `LF`.


### 2 — Build and start the containers
```bash
cd traffic-monitoring
docker compose up -d --build
```

On first run, Docker pulls ~1–2 GB of base images. Allow **3 to 5 minutes**.

Wait until all services are ready:
```bash
docker compose ps
# All services should show "healthy" or "running"
```

### 3 — Submit the Spark job
```bash
# Linux / macOS / Git Bash
chmod +x scripts/submit_job.sh
./scripts/submit_job.sh
```
```powershell
# PowerShell (native Windows)
docker exec -d spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0" `
  --conf "spark.sql.shuffle.partitions=4" `
  --conf "spark.driver.host=spark-master" `
  /opt/spark_jobs/job_traffic.py
```

> On first run, Spark downloads the Kafka connector (~30 s).
> Subsequent runs are nearly instant (JAR cached locally).

### 4 — Open the dashboard
```
http://localhost:8501
```

The first data points appear **30 to 60 seconds** after the job is submitted,
once Spark has accumulated its first matched pairs (Radar A + Radar B for the
same plate).

### 5 — Inspect the Spark cluster
```
http://localhost:8080
```

You will see the `HighwaySpeedDetection` application with its three active
streaming queries: `all_vehicles`, `violations`, `traffic_stats`.

### Stopping the project
```bash
docker compose down          # stop containers, keep checkpoints
docker compose down -v       # stop and delete Spark checkpoints
```

---

## Useful commands
```bash
# Follow logs for all services
docker compose logs -f

# Follow Kafka logs only
docker compose logs -f kafka

# Watch raw radar events (both A and B)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic radar_events

# Watch violations in real time
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic violations

# List all Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## License

This project is released under the **MIT License**.

Dependencies (Apache Spark, Apache Kafka, Streamlit) are distributed under
their own licenses (Apache License 2.0 for Spark and Kafka, Apache License 2.0
for Streamlit). This project does not redistribute any of these components.