#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

set -a
. "${ROOT_DIR}/.env"
set +a

bash "${ROOT_DIR}/scripts/download-jars.sh"
docker compose up -d postgres clickhouse spark-master

until docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
  sleep 2
done

until [[ "$(docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c "SELECT COUNT(*) FROM mock_data;" 2>/dev/null | tr -d '\r[:space:]')" == "10000" ]]; do
  sleep 2
done

docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
TRUNCATE TABLE fact_sales, dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date RESTART IDENTITY CASCADE;
"

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar \
  /opt/spark-jobs/etl_to_star.py

docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dim_customer
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"
