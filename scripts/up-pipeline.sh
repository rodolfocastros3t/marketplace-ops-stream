#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Subindo pipeline: produtor → Kafka → Spark → API"
docker compose up --build -d

echo ""
echo "API:        http://localhost:8000/health"
echo "Kafka:      localhost:19092 (external)"
echo "Dashboard:  cd frontend && npm run dev"
echo ""
echo "Logs Spark: docker compose logs -f spark"
echo "PySpark:    SPARK_ENGINE=pyspark + Dockerfile.pyspark (ver README)"
