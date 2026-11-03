"""
Job Spark Structured Streaming pour le flux de ventes.

Lit le topic Kafka `sales-events`, applique un watermark de 10 secondes,
calcule les agrégations par fenêtre tumbling de 1 minute et par magasin,
et écrit les résultats dans `sales-aggregated-spark`.

Le checkpoint permet la reprise après panne.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Configuration ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales-events")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated-spark")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints/spark")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 seconds")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")

spark = (SparkSession.builder
         .appName("sales-spark-structured-streaming")
         .config("spark.sql.session.timeZone", "UTC")
         # 200 partitions (défaut) pour 2 cœurs = surcoût énorme sur l'état
         # d'agrégation à chaque micro-lot. On aligne sur les 6 partitions Kafka.
         .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
         .getOrCreate())

# Réduire le bruit des logs
spark.sparkContext.setLogLevel("WARN")

# --- Lecture depuis Kafka ---
# Le topic est lu en mode streaming, avec startingOffsets=latest pour ne traiter
# que les nouveaux événements (le producer doit être lancé après le job).
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", INPUT_TOPIC)
       .option("startingOffsets", "latest")
       .load())

# --- Parsing JSON ---
# Le schéma doit correspondre exactement à cadrage.md.
schema = (
    "event_id STRING, "
    "event_time TIMESTAMP, "
    "emit_time TIMESTAMP, "
    "store_id STRING, "
    "product_id STRING, "
    "category STRING, "
    "quantity INT, "
    "unit_price DOUBLE, "
    "payment_method STRING"
)

events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*"))

# --- Agrégation avec watermark et fenêtre tumbling ---
# withWatermark : tolère 10 secondes de retard sur event_time.
# window("event_time", "1 minute") : fenêtres tumbling de 1 minute.
agg = (events
       .withWatermark("event_time", WATERMARK_DELAY)
       .groupBy(F.window("event_time", WINDOW_DURATION), "store_id")
       .agg(
           F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
           F.count("event_id").alias("tx_count"),
           F.sum("quantity").alias("items"),
           F.max("emit_time").alias("last_emit_time"),
       ))

# --- Écriture vers Kafka ---
# outputMode("append") : n'émet chaque fenêtre qu'une fois finalisée par le
# watermark, sans mises à jour partielles (un enregistrement par magasin).
out = (agg
       .selectExpr(
           "to_json(named_struct("
           "'window_start', window.start, "
           "'window_end', window.end, "
           "'store_id', store_id, "
           "'revenue', revenue, "
           "'tx_count', tx_count, "
           "'items', items, "
           "'last_emit_time', last_emit_time"
           ")) AS value",
           "store_id AS key",
       )
       .writeStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("topic", OUTPUT_TOPIC)
       .option("checkpointLocation", CHECKPOINT_PATH)
       .outputMode("append")
       .start())

out.awaitTermination()