#!/usr/bin/env bash
set -e

echo "⏳ 等待 Kafka 就绪..."
docker exec -i lab-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || sleep 3

echo "🚀 创建 6 分区业务订单 Topic: user_orders"
docker exec -i lab-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic user_orders \
  --partitions 6 \
  --replication-factor 1

echo "✅ Topic 创建完成！"
