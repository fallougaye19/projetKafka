#!/bin/bash
# Soumission du job Flink (PyFlink) au cluster Flink
docker exec flink-jobmanager flink run -py /opt/job/flink_job.py