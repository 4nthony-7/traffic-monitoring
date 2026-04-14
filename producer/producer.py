#!/usr/bin/env python3
__author__ = "Anthony Poher"
__date__ = "2026-04-07"

"""
Producer: Simulates two radars on a highway, generating vehicle speed data.
---------------------------------------------------------------------------
This script simulates two radars (A and B) placed 3 km apart on a highway.
It generates random vehicle license plates and speeds, 
and publishes "arrival" events to a Kafka topic : "radar_events".
---------------------------------------------------------------------------
Viewing the dashboard: open http://localhost:8501 in a browser
"""


import os
import json
import time
import random

import logging
import threading
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# ── Global ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("radar")

def generate_plate():
    """
    Generate a pool of random license plates to use in the simulation.
    """
    return (
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}-"
        f"{random.randint(100, 999)}-"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
    )

PLATE_POOL = [generate_plate() for _ in range(1000)]
RADAR_ID        = os.getenv("RADAR_ID", "A")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = os.getenv("TOPIC", "radar_events")
EMIT_INTERVAL   = int(os.getenv("EMIT_INTERVAL_MS", "600")) / 1000.0
DISTANCE_KM = 3.0
SPEED_LIMIT = 110.0
in_flight: dict = {}
in_flight_lock = threading.Lock()


# ── Functions ──────────────────────────────────────────────────────
def now_ms() -> int:
    """
    This is used to simulate the event time of radar detections.
    Returns the current timestamp in milliseconds since the Unix epoch.
    """
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def wait_for_kafka() -> KafkaProducer:
    """
    Tries to connect to Kafka, retrying every 5 seconds for up to 30 attempts.
    Returns a KafkaProducer instance if successful, or raises an error if Kafka is unreachable.
    """
    for i in range(30):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                acks="all",
            )
            log.info("Connected to Kafka (%s)", KAFKA_BOOTSTRAP)
            return p
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/30 ...", i + 1)
            time.sleep(5)
    raise RuntimeError("Kafka unreachable")


def thread_radar_a(producer: KafkaProducer):
    """
    Simulates radar A: generates random vehicles and their speeds, and publishes "departure" events to Kafka.
    Each vehicle is stored in a global dictionary with its departure time and speed, 
        so that radar B can later publish the corresponding "arrival" event after the appropriate travel time.
    """
    log.info("Radar A started")
    while True:
        plate = random.choice(PLATE_POOL)
        r = random.random()
        # 92% chances to have a vehicle traveling at a speed between 100 and 110 km/h
        if r < 0.92:
            speed = random.gauss(100, 10)
            speed = max(100, min(109, speed))
        # 8% chances to have a vehicle traveling above the speed limit, up to 160 km/h
        else:
            speed = random.uniform(SPEED_LIMIT, 160)

        ts = now_ms()
        producer.send(TOPIC, value={"plate": plate, "radar_id": "A", "ts_ms": ts})
        log.info("A -> plate=%-12s  speed=%5.1f km/h", plate, speed)

        # Store the necessary information in a global dictionary
        with in_flight_lock:
            in_flight[plate] = {
                "speed_kmh":      speed,
                "ts_depart_wall": time.time(),
                "ts_ms_a":        ts,
            }

        # Sleep for a random interval around EMIT_INTERVAL to simulate irregular traffic flow.
        time.sleep(EMIT_INTERVAL + random.uniform(0, EMIT_INTERVAL * 0.3))


def thread_radar_b(producer: KafkaProducer):
    """
    Simulates radar B: checks the global dictionary for vehicles that should have arrived based on
    their departure time and speed, and publishes "arrival" events to Kafka with the appropriate timestamp.
    """
    log.info("Radar B started — D=%.1f km", DISTANCE_KM)
    while True:
        now = time.time()
        arrived = []

        # Retrieve vehicles that should have arrived at radar B based on their departure time and speed.
        with in_flight_lock:
            for plate, info in in_flight.items():
                travel_s = (DISTANCE_KM / info["speed_kmh"]) * 3600
                if now - info["ts_depart_wall"] >= travel_s:
                    arrived.append((plate, info))
            for plate, _ in arrived:
                del in_flight[plate]

        # Publish arrival events for the vehicles that have arrived at radar B.
        for plate, info in arrived:
            travel_ms = int((DISTANCE_KM / info["speed_kmh"]) * 3_600_000)
            ts_b = info["ts_ms_a"] + travel_ms
            producer.send(TOPIC, value={"plate": plate, "radar_id": "B", "ts_ms": ts_b})
            log.info("B <- plate=%-12s  speed=%5.1f km/h  travel=%ds",
                     plate, info["speed_kmh"], travel_ms // 1000)

        # Sleep for a short interval to avoid busy-waiting, while still being responsive to new arrivals.
        time.sleep(0.5)


if __name__ == "__main__":
    producer = wait_for_kafka()

    # Run both radar threads as daemons, so they will automatically exit when the main thread is interrupted.
    ta = threading.Thread(target=thread_radar_a, args=(producer,), daemon=True)
    tb = threading.Thread(target=thread_radar_b, args=(producer,), daemon=True)
    ta.start()
    tb.start()

    log.info("Both radar threads running.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")
