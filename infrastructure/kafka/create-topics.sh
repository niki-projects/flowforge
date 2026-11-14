#!/usr/bin/env bash
set -euo pipefail
for topic in flowforge.tasks.java.v1 flowforge.tasks.kotlin.v1 flowforge.results.v1; do
  for name in "$topic" "$topic.DLT"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
      --topic "$name" --partitions 3 --replication-factor 1 \
      --config retention.ms=604800000 --config max.message.bytes=1048576
  done
done
