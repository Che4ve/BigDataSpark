#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

set -a
. "${ROOT_DIR}/.env"
set +a

wait_for_postgres() {
  until docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
    sleep 2
  done
}

wait_for_clickhouse() {
  until docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
    sleep 2
  done
}

wait_for_raw_load() {
  local count=""

  until [[ "${count}" == "10000" ]]; do
    count="$(docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c "SELECT COUNT(*) FROM mock_data;" 2>/dev/null | tr -d '\r[:space:]' || true)"
    [[ "${count}" == "10000" ]] || sleep 2
  done
}

echo "[1/6] Preparing local artifacts"
bash "${ROOT_DIR}/scripts/copy-csv-files.sh"
bash "${ROOT_DIR}/scripts/download-jars.sh"

echo "[2/6] Resetting containers and volumes"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true

echo "[3/6] Starting PostgreSQL, ClickHouse and Spark"
docker compose up -d postgres clickhouse spark-master

echo "[4/6] Waiting for databases and raw data load"
wait_for_postgres
wait_for_clickhouse
wait_for_raw_load

echo "[5/6] Running Spark ETL jobs"
bash "${ROOT_DIR}/scripts/run-etl-star.sh"
bash "${ROOT_DIR}/scripts/run-etl-clickhouse.sh"

echo "[6/6] Final verification"
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT COUNT(*) AS raw_rows FROM mock_data;
SELECT COUNT(*) AS fact_rows FROM fact_sales;
SELECT COUNT(*) AS date_rows FROM dim_date;
"

docker compose exec -T clickhouse clickhouse-client --database "${CLICKHOUSE_DB}" --query "
SELECT product_name, total_revenue
FROM mart_sales_by_product
ORDER BY total_revenue DESC
LIMIT 5;
"

echo "Pipeline completed successfully."
