"""
Générateur de flux de ventes vers Kafka.

Paramètres (variables d'environnement) :
  KAFKA_BOOTSTRAP    : adresse Kafka (défaut: kafka:9092)
  TOPIC              : topic cible (défaut: sales-events)
  EVENTS_PER_SECOND  : débit cible (défaut: 1000)
  NUM_STORES         : nombre de magasins simulés (défaut: 50)
  DURATION_SECONDS   : durée de génération (défaut: 300)
  LATE_EVENT_RATIO   : proportion d'événements tardifs (défaut: 0.0)
  LATE_DELAY_SECONDS : retard appliqué aux événements tardifs (défaut: 30)
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "sales-events")
EPS = int(os.getenv("EVENTS_PER_SECOND", "1000"))
NUM_STORES = int(os.getenv("NUM_STORES", "50"))
DURATION = int(os.getenv("DURATION_SECONDS", "300"))
LATE_RATIO = float(os.getenv("LATE_EVENT_RATIO", "0.0"))
LATE_DELAY = int(os.getenv("LATE_DELAY_SECONDS", "30"))

CATEGORIES = ["electronics", "food", "clothing", "home"]
PAYMENTS = ["card", "cash", "mobile"]


def wait_for_kafka(retries: int = 30, delay: int = 2) -> KafkaProducer:
    """Attend que Kafka soit joignable avant de créer le producer."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                linger_ms=5,
                acks="all",
            )
            print(f"[producer] Connecté à Kafka ({BOOTSTRAP})")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka indisponible, tentative {attempt}/{retries}…")
            time.sleep(delay)
    raise RuntimeError("Kafka n'est pas joignable après plusieurs tentatives")


def iso_utc(dt: datetime) -> str:
    """Format ISO 8601 UTC sans décalage, accepté par Spark et Flink."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def make_event() -> dict:
    """Construit un événement de vente conforme à cadrage.md."""
    now = datetime.now(timezone.utc)
    is_late = random.random() < LATE_RATIO

    if is_late:
        event_time = now - timedelta(seconds=LATE_DELAY)
    else:
        event_time = now

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": iso_utc(event_time),
        "emit_time": iso_utc(now),
        "store_id": f"S{random.randint(1, NUM_STORES):03d}",
        "product_id": f"P{random.randint(1, 500):04d}",
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(5.0, 200.0), 2),
        "payment_method": random.choice(PAYMENTS),
    }


def main() -> None:
    producer = wait_for_kafka()
    start = time.time()
    interval = 1.0 / EPS
    sent = 0

    print(f"[producer] Démarrage : {EPS} ev/s pendant {DURATION}s "
          f"({NUM_STORES} magasins, LATE_RATIO={LATE_RATIO})")

    while time.time() - start < DURATION:
        ev = make_event()
        producer.send(TOPIC, key=ev["store_id"], value=ev)
        sent += 1

        # Régulation du débit : on vise start + sent * interval
        target = start + sent * interval
        sleep = target - time.time()
        if sleep > 0:
            time.sleep(sleep)

    producer.flush()
    elapsed = time.time() - start
    actual = sent / elapsed
    print(f"[producer] Terminé : {sent} événements en {elapsed:.1f}s "
          f"(débit effectif {actual:.0f} ev/s)")


if __name__ == "__main__":
    main()