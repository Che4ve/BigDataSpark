# Лабораторная работа №2 по дисциплине «Анализ больших данных»

Проект реализует полный ETL-пайплайн на `PostgreSQL + Apache Spark + ClickHouse`.

Поток данных такой:

1. 10 CSV-файлов с исходными данными копируются в каталог для PostgreSQL.
2. PostgreSQL автоматически создаёт сырую таблицу `mock_data`, схему «звезда» и загружает `10000` строк.
3. Spark-джоба `etl_to_star.py` переносит данные из raw-слоя в таблицы измерений и фактов PostgreSQL.
4. Spark-джоба `etl_to_clickhouse.py` строит 6 аналитических витрин в ClickHouse.

## Состав проекта

- `docker-compose.yml` — PostgreSQL, ClickHouse и контейнер со Spark.
- `postgres/init/` — DDL raw-слоя и схемы «звезда», а также `COPY` для загрузки CSV.
- `clickhouse/init/` — DDL витрин ClickHouse.
- `spark/etl_to_star.py` — ETL `mock_data -> star schema`.
- `spark/etl_to_clickhouse.py` — ETL `star schema -> ClickHouse marts`.
- `scripts/copy-csv-files.sh` — копирует исходные CSV в `postgres/data/csv/`.
- `scripts/download-jars.sh` — скачивает JDBC-драйверы PostgreSQL и ClickHouse.
- `scripts/run-etl-star.sh` — запускает только первую Spark-джобу.
- `scripts/run-etl-clickhouse.sh` — запускает только вторую Spark-джобу.
- `scripts/run-all.sh` — полный сценарий от старта контейнеров до проверочных запросов.
- `report.md` — отчёт по проделанной работе.

## Что используется

- `PostgreSQL 15`
- `ClickHouse 24.3`
- `Apache Spark 4.0.2` в контейнере `apache/spark`

Spark запускается в контейнере `spark-master`, а сами джобы выполняются через `docker compose exec` в режиме `local[*]`. Для датасета из `10000` строк этого достаточно, а сценарий остаётся полностью воспроизводимым через Docker Compose.

## Требования

- Docker
- Docker Compose
- доступ в интернет на первом запуске, чтобы скачать JDBC jars

## Быстрый запуск

Полный сценарий:

```bash
./scripts/run-all.sh
```

Скрипт делает следующее:

1. копирует CSV в `postgres/data/csv/`;
2. скачивает JDBC-драйверы в `spark/jars/`;
3. выполняет `docker compose down -v --remove-orphans`;
4. поднимает PostgreSQL, ClickHouse и Spark;
5. ждёт загрузки `10000` строк в `mock_data`;
6. запускает обе Spark-джобы;
7. выполняет финальные проверочные запросы.

Важно: `run-all.sh` сбрасывает docker volumes. Если нужны данные от предыдущего запуска, используйте раздельный запуск без `down -v`.

## Раздельный запуск

Если нужно прогнать этапы по отдельности:

```bash
./scripts/copy-csv-files.sh
./scripts/download-jars.sh
docker compose up -d postgres clickhouse spark-master
./scripts/run-etl-star.sh
./scripts/run-etl-clickhouse.sh
```

Особенности:

- `run-etl-star.sh` очищает таблицы схемы «звезда» в PostgreSQL и заполняет их заново.
- `run-etl-clickhouse.sh` очищает 6 витрин в ClickHouse и строит их заново.

## Параметры подключения

Все переменные лежат в `.env`.

Доступ с хоста:

- PostgreSQL: `localhost:15432`
- ClickHouse HTTP: `localhost:18123`
- ClickHouse Native: `localhost:19000`

Учётные данные:

- БД: `bigdata`
- пользователь: `spark`
- пароль: `spark123`

## Как проверить лабораторную

### 1. Проверка raw-слоя и схемы «звезда» в PostgreSQL

```bash
docker compose exec -T postgres psql -U spark -d bigdata -c "
SELECT COUNT(*) AS raw_rows FROM mock_data;
SELECT COUNT(*) AS fact_rows FROM fact_sales;
SELECT COUNT(*) AS date_rows FROM dim_date;
"
```

Ожидаемый результат после полного запуска:

- `mock_data`: `10000`
- `fact_sales`: `10000`
- `dim_date`: `364`

Проверка размеров всех таблиц «звезды»:

```bash
docker compose exec -T postgres psql -U spark -d bigdata -c "
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
```

### 2. Проверка витрин в ClickHouse

Проверка количества строк:

```bash
docker compose exec -T clickhouse clickhouse-client --database bigdata --query "
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
```

Ожидаемый результат:

- `mart_sales_by_product`: `10000`
- `mart_sales_by_customer`: `10000`
- `mart_sales_by_time`: `12`
- `mart_sales_by_store`: `10000`
- `mart_sales_by_supplier`: `10000`
- `mart_product_quality`: `10000`

Пример проверочного запроса для витрины продаж по времени:

```bash
docker compose exec -T clickhouse clickhouse-client --database bigdata --query "
SELECT year, month, month_name, total_revenue, avg_order_value
FROM mart_sales_by_time
ORDER BY year, month;
"
```

Пример проверочного запроса для витрины продаж по продуктам:

```bash
docker compose exec -T clickhouse clickhouse-client --database bigdata --query "
SELECT product_name, total_revenue, product_rank
FROM mart_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
"
```

## Логика модели данных

### PostgreSQL raw-слой

Сырая таблица называется `mock_data`. Даты (`sale_date`, `product_release_date`, `product_expiry_date`) сохраняются как строки и приводятся к типу `DATE` уже в Spark, потому что исходный формат имеет вид `M/d/yyyy`.

### PostgreSQL schema star

Создаются таблицы:

- `dim_customer`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`
- `dim_date`
- `fact_sales`

Суррогатные ключи в измерениях создаются в Spark через:

- хеш естественного ключа `sha2(...)`
- `row_number()` по отсортированному хешу

Это сделано потому, что в mock-данных многие исходные идентификаторы и комбинации полей не подходят как стабильные бизнес-ключи между файлами.

### ClickHouse marts

Создаются 6 витрин:

- `mart_sales_by_product`
- `mart_sales_by_customer`
- `mart_sales_by_time`
- `mart_sales_by_store`
- `mart_sales_by_supplier`
- `mart_product_quality`

## Полезные замечания

- При первом запуске JDBC jars скачиваются автоматически.
- Имена исходных CSV содержат пробелы и скобки, поэтому перед загрузкой они копируются в безопасные имена `mock_data_0.csv ... mock_data_9.csv`.
- В логах Spark возможны предупреждения `WindowExec: No Partition Defined`. Для учебного датасета на `10000` строк это допустимо.
- В витринах по продуктам могут встречаться одинаковые `product_name`, потому что зерно продукта в звезде фактически соответствует отдельному SKU: у строк с одинаковым названием отличаются другие атрибуты товара.

## Итог

После выполнения `./scripts/run-all.sh` лабораторная полностью готова к проверке:

- данные загружены в PostgreSQL;
- схема «звезда» заполнена;
- 6 витрин созданы в ClickHouse;
- есть инструкции по запуску и контрольные запросы.
