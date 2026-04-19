import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def log(message: str) -> None:
    print(f"[etl_to_star] {message}", flush=True)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ETL Raw to Star Schema")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def jdbc_url() -> str:
    return "jdbc:postgresql://{host}:{port}/{db}".format(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "bigdata"),
    )


def jdbc_properties() -> dict:
    return {
        "user": os.getenv("POSTGRES_USER", "spark"),
        "password": os.getenv("POSTGRES_PASSWORD", "spark123"),
        "driver": "org.postgresql.Driver",
    }


def trim_string_columns(df):
    string_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string"]
    for column_name in string_columns:
        df = df.withColumn(column_name, F.trim(F.col(column_name)))
    return df


def natural_key_expr(columns: list[str]):
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in columns]),
        256,
    )


def build_dimension(raw_df, aliases, key_column, natural_key_column):
    renamed_columns = [alias for _, alias in aliases]
    dim_df = raw_df.select(*[F.col(source).alias(alias) for source, alias in aliases])
    dim_df = dim_df.withColumn(natural_key_column, natural_key_expr(renamed_columns))
    dim_df = dim_df.dropDuplicates([natural_key_column])

    window = Window.orderBy(F.col(natural_key_column))
    dim_df = dim_df.withColumn(key_column, F.row_number().over(window))

    return dim_df.select(key_column, *renamed_columns, natural_key_column)


def main() -> None:
    spark = build_spark_session()
    pg_url = jdbc_url()
    pg_props = jdbc_properties()

    log("Reading raw table mock_data from PostgreSQL")
    raw_df = spark.read.jdbc(pg_url, "mock_data", properties=pg_props)
    raw_df = trim_string_columns(raw_df)
    raw_df = (
        raw_df.withColumn("sale_date_parsed", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn("release_date_parsed", F.to_date("product_release_date", "M/d/yyyy"))
        .withColumn("expiry_date_parsed", F.to_date("product_expiry_date", "M/d/yyyy"))
    ).cache()

    raw_count = raw_df.count()
    invalid_sale_dates = raw_df.filter(F.col("sale_date_parsed").isNull()).count()
    log(f"Loaded {raw_count} rows from mock_data")
    log(f"Rows with invalid sale_date after parsing: {invalid_sale_dates}")

    customer_columns = [
        ("customer_first_name", "first_name"),
        ("customer_last_name", "last_name"),
        ("customer_age", "age"),
        ("customer_email", "email"),
        ("customer_country", "country"),
        ("customer_postal_code", "postal_code"),
        ("customer_pet_type", "pet_type"),
        ("customer_pet_name", "pet_name"),
        ("customer_pet_breed", "pet_breed"),
        ("pet_category", "pet_category"),
    ]
    seller_columns = [
        ("seller_first_name", "first_name"),
        ("seller_last_name", "last_name"),
        ("seller_email", "email"),
        ("seller_country", "country"),
        ("seller_postal_code", "postal_code"),
    ]
    product_columns = [
        ("product_name", "name"),
        ("product_category", "category"),
        ("product_price", "price"),
        ("product_quantity", "quantity"),
        ("product_weight", "weight"),
        ("product_color", "color"),
        ("product_size", "size"),
        ("product_brand", "brand"),
        ("product_material", "material"),
        ("product_description", "description"),
        ("product_rating", "rating"),
        ("product_reviews", "reviews"),
        ("release_date_parsed", "release_date"),
        ("expiry_date_parsed", "expiry_date"),
    ]
    store_columns = [
        ("store_name", "name"),
        ("store_location", "location"),
        ("store_city", "city"),
        ("store_state", "state"),
        ("store_country", "country"),
        ("store_phone", "phone"),
        ("store_email", "email"),
    ]
    supplier_columns = [
        ("supplier_name", "name"),
        ("supplier_contact", "contact"),
        ("supplier_email", "email"),
        ("supplier_phone", "phone"),
        ("supplier_address", "address"),
        ("supplier_city", "city"),
        ("supplier_country", "country"),
    ]

    dim_customer = build_dimension(raw_df, customer_columns, "customer_id", "customer_nk")
    dim_seller = build_dimension(raw_df, seller_columns, "seller_id", "seller_nk")
    dim_product = build_dimension(raw_df, product_columns, "product_id", "product_nk")
    dim_store = build_dimension(raw_df, store_columns, "store_id", "store_nk")
    dim_supplier = build_dimension(raw_df, supplier_columns, "supplier_id", "supplier_nk")

    dim_date = (
        raw_df.select(F.col("sale_date_parsed").alias("full_date"))
        .where(F.col("sale_date_parsed").isNotNull())
        .distinct()
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("date_id", F.row_number().over(Window.orderBy("full_date")))
        .select("date_id", "full_date", "day", "month", "year", "quarter", "day_of_week", "month_name")
    )

    log("Building fact table with surrogate keys")
    fact_sales = (
        raw_df.withColumn("customer_nk", natural_key_expr([source for source, _ in customer_columns]))
        .withColumn("seller_nk", natural_key_expr([source for source, _ in seller_columns]))
        .withColumn("product_nk", natural_key_expr([source for source, _ in product_columns]))
        .withColumn("store_nk", natural_key_expr([source for source, _ in store_columns]))
        .withColumn("supplier_nk", natural_key_expr([source for source, _ in supplier_columns]))
        .withColumn("full_date", F.col("sale_date_parsed"))
        .join(dim_customer.select("customer_id", "customer_nk"), "customer_nk", "inner")
        .join(dim_seller.select("seller_id", "seller_nk"), "seller_nk", "inner")
        .join(dim_product.select("product_id", "product_nk"), "product_nk", "inner")
        .join(dim_store.select("store_id", "store_nk"), "store_nk", "inner")
        .join(dim_supplier.select("supplier_id", "supplier_nk"), "supplier_nk", "inner")
        .join(dim_date.select("date_id", "full_date"), "full_date", "inner")
        .select(
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            "supplier_id",
            "date_id",
            F.col("sale_quantity").cast("int").alias("sale_quantity"),
            F.col("sale_total_price").cast("decimal(14,2)").alias("sale_total_price"),
        )
    )

    log("Writing dimensions and fact table to PostgreSQL")
    dim_customer.drop("customer_nk").write.jdbc(pg_url, "dim_customer", mode="append", properties=pg_props)
    dim_seller.drop("seller_nk").write.jdbc(pg_url, "dim_seller", mode="append", properties=pg_props)
    dim_product.drop("product_nk").write.jdbc(pg_url, "dim_product", mode="append", properties=pg_props)
    dim_store.drop("store_nk").write.jdbc(pg_url, "dim_store", mode="append", properties=pg_props)
    dim_supplier.drop("supplier_nk").write.jdbc(pg_url, "dim_supplier", mode="append", properties=pg_props)
    dim_date.write.jdbc(pg_url, "dim_date", mode="append", properties=pg_props)
    fact_sales.write.jdbc(pg_url, "fact_sales", mode="append", properties=pg_props)

    log("Star schema load completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()
