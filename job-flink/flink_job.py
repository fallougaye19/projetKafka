
"""
Job PyFlink Table API : agrégation des ventes en temps réel.
- Source Kafka : sales-events
- Watermark : 10 secondes
- Fenêtres temporelles : 1 minute par magasin
- Agrégations : chiffre d'affaires, transactions, articles, dernière
  date d'émission
- Sink Kafka : sales-aggregated-flink
"""
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales-events")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated-flink")

KAFKA_CONNECTOR_JAR = os.getenv(
    "KAFKA_CONNECTOR_JAR",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar",
)

CHECKPOINT_INTERVAL_MS = int(
    os.getenv("CHECKPOINT_INTERVAL_MS", "10000")
)
# Même valeur que Spark (WATERMARK_DELAY) pour une comparaison équitable.
WATERMARK_DELAY_S = int(os.getenv("WATERMARK_DELAY_S", "10"))
PARALLELISM = int(os.getenv("PARALLELISM", "2"))

# Environnement Flink
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
env.set_parallelism(PARALLELISM)
env.add_jars(KAFKA_CONNECTOR_JAR)

settings = (
    EnvironmentSettings.new_instance()
    .in_streaming_mode()
    .build()
)

t_env = StreamTableEnvironment.create(
    env, environment_settings=settings
)

t_env.get_config().set(
    "table.exec.source.idle-timeout", "1 min"
)

# Source Kafka
source_ddl = f"""
CREATE TABLE sales_source (
    event_id STRING,
    event_time TIMESTAMP(3),
    emit_time TIMESTAMP(3),
    store_id STRING,
    product_id STRING,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    payment_method STRING,

    WATERMARK FOR event_time AS
        event_time - INTERVAL '{WATERMARK_DELAY_S}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{INPUT_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'properties.group.id' = 'flink-sales-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false',
    'json.timestamp-format.standard' = 'ISO-8601'
)
"""
t_env.execute_sql(source_ddl)

# Sink Kafka
sink_ddl = f"""
CREATE TABLE sales_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    store_id STRING,
    revenue DOUBLE,
    tx_count BIGINT,
    items BIGINT,
    last_emit_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = '{OUTPUT_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
    'key.format' = 'raw',
    'key.fields' = 'store_id',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
"""
t_env.execute_sql(sink_ddl)

# Agrégation par fenêtre événementielle d'une minute
insert_sql = """
INSERT INTO sales_sink
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE)
        AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE)
        AS window_end,
    store_id,
    SUM(quantity * unit_price) AS revenue,
    COUNT(*) AS tx_count,
    SUM(quantity) AS items,
    MAX(emit_time) AS last_emit_time
FROM sales_source
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    store_id
"""

t_env.execute_sql(insert_sql)
