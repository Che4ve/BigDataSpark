CREATE DATABASE IF NOT EXISTS bigdata;
CREATE USER IF NOT EXISTS spark IDENTIFIED WITH plaintext_password BY 'spark123';
GRANT ALL ON bigdata.* TO spark;

CREATE TABLE IF NOT EXISTS bigdata.mart_sales_by_product (
    product_id          Int32,
    product_name        String,
    product_category    String,
    product_brand       String,
    total_quantity_sold Int64,
    total_revenue       Float64,
    avg_price           Float64,
    avg_rating          Float64,
    avg_reviews         Float64,
    product_rank        Int32
) ENGINE = MergeTree()
ORDER BY (product_category, total_revenue, product_id);

CREATE TABLE IF NOT EXISTS bigdata.mart_sales_by_customer (
    customer_id      Int32,
    customer_name    String,
    customer_email   String,
    customer_country String,
    customer_age     Int32,
    total_purchases  Int64,
    total_spent      Float64,
    avg_check        Float64,
    customer_rank    Int32
) ENGINE = MergeTree()
ORDER BY (customer_country, total_spent, customer_id);

CREATE TABLE IF NOT EXISTS bigdata.mart_sales_by_time (
    year            Int32,
    month           Int32,
    month_name      String,
    quarter         Int32,
    total_sales     Int64,
    total_revenue   Float64,
    avg_order_size  Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS bigdata.mart_sales_by_store (
    store_id       Int32,
    store_name     String,
    store_city     String,
    store_country  String,
    total_sales    Int64,
    total_revenue  Float64,
    avg_check      Float64,
    store_rank     Int32
) ENGINE = MergeTree()
ORDER BY (store_country, total_revenue, store_id);

CREATE TABLE IF NOT EXISTS bigdata.mart_sales_by_supplier (
    supplier_id          Int32,
    supplier_name        String,
    supplier_country     String,
    total_products_sold  Int64,
    total_revenue        Float64,
    avg_product_price    Float64,
    supplier_rank        Int32
) ENGINE = MergeTree()
ORDER BY (supplier_country, total_revenue, supplier_id);

CREATE TABLE IF NOT EXISTS bigdata.mart_product_quality (
    product_id                 Int32,
    product_name               String,
    product_category           String,
    product_brand              String,
    avg_rating                 Float64,
    total_reviews              Int64,
    total_sales_qty            Int64,
    total_revenue              Float64,
    rating_rank_best           Int32,
    rating_rank_worst          Int32,
    rating_sales_correlation   Float64
) ENGINE = MergeTree()
ORDER BY (product_category, avg_rating, product_id);
