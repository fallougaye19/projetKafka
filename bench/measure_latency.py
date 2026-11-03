"""
Mesure de la latence de bout en bout sur le topic de sortie (Spark ou Flink).

Pour chaque résultat reçu : latence = heure de réception - last_emit_time
(dernier emit_time des événements de la fenêtre). Écrit :
  - results/raw/<moteur>_<palier>.csv      un enregistrement par résultat reçu
  - results/latency/<moteur>_<palier>.csv  résumé p50 / p95

Variables d'environnement : KAFKA_BOOTSTRAP, TOPIC, MOTEUR, PALIER, RATE,
MEASURE_DURATION (secondes).
"""
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MOTEUR = os.getenv("MOTEUR", "spark")
TOPIC = os.getenv("TOPIC", f"sales-aggregated-{MOTEUR}")
PALIER = os.getenv("PALIER", "P1")
RATE = os.getenv("RATE", "")
DURATION = int(os.getenv("MEASURE_DURATION", "240"))
RAW_CSV = os.getenv("RAW_CSV", f"results/raw/{MOTEUR}_{PALIER}.csv")
SUMMARY_CSV = os.getenv("OUTPUT_CSV", f"results/latency/{MOTEUR}_{PALIER}.csv")


def parse_iso(ts: str) -> float:
    """ISO 8601 -> epoch secondes UTC.

    Accepte un 'Z' final ou aucun suffixe (Flink n'en écrit pas) et une
    fraction de 0 à 9 chiffres (Flink supprime les zéros finaux : '.97').
    Un horodatage sans fuseau est toujours interprété en UTC.
    """
    ts = ts.rstrip("Z")
    base, _, frac = ts.partition(".")
    dt = datetime.fromisoformat(f"{base}.{frac.ljust(6, '0')[:6]}")
    return dt.replace(tzinfo=timezone.utc).timestamp()


def hhmmss() -> str:
    return time.strftime("%H:%M:%S", time.gmtime()) + " UTC"


def main() -> None:
    t_start = time.time()
    print(f"[measure] python demarre a {hhmmss()}", flush=True)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"[measure] consommateur cree (+{time.time() - t_start:.0f}s)", flush=True)

    # Attendre l'assignation des partitions et FIXER la position 'latest' avant
    # de dire PRET : le producer ne démarre qu'après ce marqueur.
    while not consumer.assignment():
        consumer.poll(timeout_ms=500)
    for tp in consumer.assignment():
        consumer.position(tp)
    print(f"[measure] PRET (+{time.time() - t_start:.0f}s) a {hhmmss()} - "
          f"{MOTEUR} {PALIER} ({RATE} ev/s) : lecture de {TOPIC} pendant "
          f"{DURATION}s, partitions={len(consumer.assignment())}", flush=True)

    os.makedirs(os.path.dirname(RAW_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(SUMMARY_CSV), exist_ok=True)

    received = 0
    errors = 0
    latencies = []
    t0 = time.time()
    deadline = t0 + DURATION
    next_hb = t0 + 30

    with open(RAW_CSV, "w", encoding="utf-8", newline="") as raw:
        w = csv.writer(raw)
        w.writerow(["moteur", "palier", "rate", "recv_epoch", "window_start",
                    "window_end", "store_id", "tx_count", "items", "revenue",
                    "last_emit_epoch", "latency_ms"])
        while time.time() < deadline:
            batch = consumer.poll(timeout_ms=1000)
            now = time.time()
            if now >= next_hb:
                next_hb += 30
                try:
                    parts = list(consumer.assignment())
                    ends = consumer.end_offsets(parts) if parts else {}
                    print(f"[measure] t={int(now - t0)}s partitions={len(parts)} "
                          f"offsets_sortie_total={sum(ends.values())} recus={received}",
                          flush=True)
                except Exception as exc:  # affichage de diagnostic uniquement
                    print(f"[measure] t={int(now - t0)}s (diagnostic indisponible : {exc})",
                          flush=True)
            for msgs in batch.values():
                for m in msgs:
                    r = m.value
                    received += 1
                    try:
                        last_emit = parse_iso(r["last_emit_time"])
                        lat_ms = (now - last_emit) * 1000
                        w.writerow([MOTEUR, PALIER, RATE, f"{now:.3f}",
                                    f"{parse_iso(r['window_start']):.3f}",
                                    f"{parse_iso(r['window_end']):.3f}",
                                    r["store_id"], r["tx_count"], r["items"],
                                    r["revenue"], f"{last_emit:.3f}",
                                    f"{lat_ms:.1f}"])
                        latencies.append(lat_ms)
                    except (KeyError, ValueError) as exc:
                        errors += 1
                        if errors <= 3:
                            print(f"[measure] message ignoré ({exc}) : {r}",
                                  flush=True)
    consumer.close()

    print(f"[measure] reçus={received}, ignorés={errors}, "
          f"gardés={len(latencies)}", flush=True)
    if not latencies:
        print("[measure] Aucune mesure collectée.", flush=True)
        sys.exit(1)

    p50 = float(np.median(latencies))
    p95 = float(np.percentile(latencies, 95))
    with open(SUMMARY_CSV, "w", encoding="utf-8") as f:
        f.write("moteur,palier,timestamp,nb_mesures,latence_p50_ms,latence_p95_ms\n")
        f.write(f"{MOTEUR},{PALIER},{datetime.now(timezone.utc).isoformat()},"
                f"{len(latencies)},{p50:.2f},{p95:.2f}\n")
    print(f"[measure] {len(latencies)} mesures - p50={p50:.0f} ms, "
          f"p95={p95:.0f} ms -> {RAW_CSV}", flush=True)


if __name__ == "__main__":
    main()
