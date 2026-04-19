#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="${ROOT_DIR}/исходные данные"
DST_DIR="${ROOT_DIR}/postgres/data/csv"

mkdir -p "${DST_DIR}"
rm -f "${DST_DIR}"/mock_data_*.csv

cp "${SRC_DIR}/MOCK_DATA.csv" "${DST_DIR}/mock_data_0.csv"

for i in $(seq 1 9); do
  cp "${SRC_DIR}/MOCK_DATA (${i}).csv" "${DST_DIR}/mock_data_${i}.csv"
done

echo "CSV files copied to ${DST_DIR}"
