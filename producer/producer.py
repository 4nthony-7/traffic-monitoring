#!/usr/bin/env python3
"""
Radar Event Producer
====================
Simule les deux radars A et B dans UN seul processus Python.

Radar A et Radar B partagent un dict en mémoire (in_flight).
Radar A génère des véhicules et les stocke dans in_flight.
Radar B surveille in_flight et publie l'arrivée une fois le
temps de trajet écoulé.

Les deux radars publient sur le même topic Kafka : radar_events.
  { "plate": "AB-123-CD", "radar_id": "A"|"B", "ts_ms": <epoch ms> }

Seul le conteneur producer-radar-a fait le travail (RADAR_ID=A).
Le conteneur producer-radar-b reste en veille (inclus pour la lisibilité
du docker-compose).
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("radar")

RADAR_ID        = os.getenv("RADAR_ID", "A")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = os.getenv("TOPIC", "radar_events")
EMIT_INTERVAL   = int(os.getenv("EMIT_INTERVAL_MS", "800")) / 1000.0

DISTANCE_KM = 3    # Distance entre les radars (pour calcul du temps de trajet)


# GENERER UNE PLAQUE D'IMMATRICULATION ALÉATOIRE
def generate_plate():
    return (
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}-"
        f"{random.randint(100, 999)}-"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
        f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
    )
PLATE_POOL = [generate_plate() for _ in range(500)]

in_flight: dict = {}
in_flight_lock = threading.Lock()


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def wait_for_kafka() -> KafkaProducer:
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
    log.info("Radar A started")
    while True:
        plate = random.choice(PLATE_POOL)
        r = random.random()
        if r < 0.92:
            speed = random.gauss(100, 10)
            speed = max(100, min(109, speed))
        else:
            speed = random.uniform(110, 160)

        ts = now_ms()
        producer.send(TOPIC, value={"plate": plate, "radar_id": "A", "ts_ms": ts})
        log.info("A -> plate=%-12s  speed=%5.1f km/h", plate, speed)

        with in_flight_lock:
            in_flight[plate] = {
                "speed_kmh":      speed,
                "ts_depart_wall": time.time(),
                "ts_ms_a":        ts,
            }

        time.sleep(EMIT_INTERVAL + random.uniform(0, EMIT_INTERVAL * 0.3))


def thread_radar_b(producer: KafkaProducer):
    log.info("Radar B started — D=%.1f km", DISTANCE_KM)
    while True:
        now = time.time()
        arrived = []

        with in_flight_lock:
            for plate, info in in_flight.items():
                travel_s = (DISTANCE_KM / info["speed_kmh"]) * 3600
                if now - info["ts_depart_wall"] >= travel_s:
                    arrived.append((plate, info))
            for plate, _ in arrived:
                del in_flight[plate]

        for plate, info in arrived:
            travel_ms = int((DISTANCE_KM / info["speed_kmh"]) * 3_600_000)
            ts_b = info["ts_ms_a"] + travel_ms
            producer.send(TOPIC, value={"plate": plate, "radar_id": "B", "ts_ms": ts_b})
            log.info("B <- plate=%-12s  speed=%5.1f km/h  travel=%ds",
                     plate, info["speed_kmh"], travel_ms // 1000)

        time.sleep(0.5)


if __name__ == "__main__":
    if RADAR_ID != "A":
        log.info("RADAR_ID=%s — en veille (les deux radars tournent dans producer-radar-a)", RADAR_ID)
        while True:
            time.sleep(3600)

    producer = wait_for_kafka()

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
