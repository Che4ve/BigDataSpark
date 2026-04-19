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

until docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

until docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c "SELECT COUNT(*) FROM fact_sales;" >/dev/null 2>&1; do
  sleep 2
done

docker compose exec -T clickhouse clickhouse-client --multiquery --database "${CLICKHOUSE_DB}" --query "
TRUNCATE TABLE mart_sales_by_product;
TRUNCATE TABLE mart_sales_by_customer;
TRUNCATE TABLE mart_sales_by_time;
TRUNCATE TABLE mart_sales_by_store;
TRUNCATE TABLE mart_sales_by_supplier;
TRUNCATE TABLE mart_product_quality;
"

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar,/opt/spark-jobs/jars/clickhouse-jdbc-0.6.0-patch5-all.jar \
  /opt/spark-jobs/etl_to_clickhouse.py

docker compose exec -T clickhouse clickhouse-client --database "${CLICKHOUSE_DB}" --query "
SELECT 'mart_sales_by_product' AS table_name, count() AS row_count FROM mart_sales_by_product
UNION ALL
SELECT 'mart_sales_by_customer', count() FROM mart_sales_by_customer
UNION ALL
SELECT 'mart_sales_by_time', count() FROM mart_sales_by_time
UNION ALL
SELECT 'mart_sales_by_store', count() FROM mart_sales_by_store
UNION ALL
SELECT 'mart_sales_by_supplier', count() FROM mart_sales_by_supplier
UNION ALL
SELECT 'mart_product_quality', count() FROM mart_product_quality;
"
