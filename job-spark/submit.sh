#!/bin/bash
# Soumission du job Spark au cluster Spark (mode standalone)
# Un checkpoint unique par lancement évite les conflits entre jobs concurrents.
# Pour reprendre après panne, relancer avec le même CHECKPOINT_PATH.
if docker exec spark-master pgrep -f spark_job.py > /dev/null; then
  echo "Un job Spark tourne déjà. Arrêtez-le avant de soumettre :" >&2
  echo "  docker exec spark-master bash -c 'pkill -f spark_job.py; pkill -f SparkSubmit'" >&2
  exit 1
fi

RUN_ID=$(date +%Y%m%d-%H%M%S)
LOG_FILE="/tmp/spark-$RUN_ID.log"

docker exec -d \
  -e CHECKPOINT_PATH="/tmp/checkpoints/spark-$RUN_ID" \
  spark-master bash -c "/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/job/spark_job.py > $LOG_FILE 2>&1"

echo "Job soumis. Suivi des logs :"
echo "  docker exec spark-master tail -f $LOG_FILE"
