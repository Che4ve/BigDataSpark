CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    age         INTEGER,
    email       VARCHAR(200),
    country     VARCHAR(100),
    postal_code VARCHAR(20),
    pet_type    VARCHAR(50),
    pet_name    VARCHAR(100),
    pet_breed   VARCHAR(100),
    pet_category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id   SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200),
    country     VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   SERIAL PRIMARY KEY,
    name         VARCHAR(100),
    category     VARCHAR(50),
    price        NUMERIC(12, 2),
    quantity     INTEGER,
    weight       NUMERIC(12, 2),
    color        VARCHAR(50),
    size         VARCHAR(20),
    brand        VARCHAR(100),
    material     VARCHAR(100),
    description  TEXT,
    rating       NUMERIC(4, 1),
    reviews      INTEGER,
    release_date DATE,
    expiry_date  DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id  SERIAL PRIMARY KEY,
    name      VARCHAR(200),
    location  VARCHAR(200),
    city      VARCHAR(200),
    state     VARCHAR(200),
    country   VARCHAR(100),
    phone     VARCHAR(50),
    email     VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name        VARCHAR(200),
    contact     VARCHAR(200),
    email       VARCHAR(200),
    phone       VARCHAR(50),
    address     VARCHAR(200),
    city        VARCHAR(200),
    country     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE UNIQUE,
    day         INTEGER,
    month       INTEGER,
    year        INTEGER,
    quarter     INTEGER,
    day_of_week INTEGER,
    month_name  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id           SERIAL PRIMARY KEY,
    customer_id       INTEGER REFERENCES dim_customer(customer_id),
    seller_id         INTEGER REFERENCES dim_seller(seller_id),
    product_id        INTEGER REFERENCES dim_product(product_id),
    store_id          INTEGER REFERENCES dim_store(store_id),
    supplier_id       INTEGER REFERENCES dim_supplier(supplier_id),
    date_id           INTEGER REFERENCES dim_date(date_id),
    sale_quantity     INTEGER,
    sale_total_price  NUMERIC(14, 2)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id ON fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller_id ON fact_sales (seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store_id ON fact_sales (store_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier_id ON fact_sales (supplier_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_id ON fact_sales (date_id);
