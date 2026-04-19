#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JARS_DIR="${ROOT_DIR}/spark/jars"

mkdir -p "${JARS_DIR}"

download_if_missing() {
  local file_name="$1"
  local url="$2"

  if [[ ! -f "${JARS_DIR}/${file_name}" ]]; then
    echo "Downloading ${file_name}..."
    curl -fsSL "${url}" -o "${JARS_DIR}/${file_name}"
  else
    echo "${file_name} already exists, skipping."
  fi
}

download_if_missing \
  "postgresql-42.7.3.jar" \
  "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

download_if_missing \
  "clickhouse-jdbc-0.6.0-patch5-all.jar" \
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0-patch5/clickhouse-jdbc-0.6.0-patch5-all.jar"

echo "JDBC jars are ready in ${JARS_DIR}"
