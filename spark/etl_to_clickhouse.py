import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def log(message: str) -> None:
    print(f"[etl_to_clickhouse] {message}", flush=True)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ETL Star Schema to ClickHouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def pg_url() -> str:
    return "jdbc:postgresql://{host}:{port}/{db}".format(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "bigdata"),
    )


def pg_props() -> dict:
    return {
        "user": os.getenv("POSTGRES_USER", "spark"),
        "password": os.getenv("POSTGRES_PASSWORD", "spark123"),
        "driver": "org.postgresql.Driver",
    }


def ch_url() -> str:
    return "jdbc:clickhouse://{host}:{port}/{db}".format(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=os.getenv("CLICKHOUSE_HTTP_PORT", "8123"),
        db=os.getenv("CLICKHOUSE_DB", "bigdata"),
    )


def ch_props() -> dict:
    return {
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }


def cast_metrics(df, float_columns=None, long_columns=None, int_columns=None):
    float_columns = float_columns or []
    long_columns = long_columns or []
    int_columns = int_columns or []

    for column_name in float_columns:
        df = df.withColumn(column_name, F.col(column_name).cast("double"))
    for column_name in long_columns:
        df = df.withColumn(column_name, F.col(column_name).cast("long"))
    for column_name in int_columns:
        df = df.withColumn(column_name, F.col(column_name).cast("int"))
    return df


def main() -> None:
    spark = build_spark_session()
    postgres_url = pg_url()
    postgres_props = pg_props()
    clickhouse_url = ch_url()
    clickhouse_props = ch_props()

    log("Reading star schema tables from PostgreSQL")
    fact_sales = spark.read.jdbc(postgres_url, "fact_sales", properties=postgres_props)
    dim_customer = spark.read.jdbc(postgres_url, "dim_customer", properties=postgres_props)
    dim_product = spark.read.jdbc(postgres_url, "dim_product", properties=postgres_props)
    dim_store = spark.read.jdbc(postgres_url, "dim_store", properties=postgres_props)
    dim_supplier = spark.read.jdbc(postgres_url, "dim_supplier", properties=postgres_props)
    dim_date = spark.read.jdbc(postgres_url, "dim_date", properties=postgres_props)

    log("Building marts")
    sales_by_product = (
        fact_sales.join(dim_product, "product_id")
        .groupBy("product_id", "name", "category", "brand")
        .agg(
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.avg("rating").alias("avg_rating"),
            F.avg("reviews").alias("avg_reviews"),
        )
        .withColumn("product_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"), F.asc("product_id"))))
        .withColumnRenamed("name", "product_name")
        .withColumnRenamed("category", "product_category")
        .withColumnRenamed("brand", "product_brand")
    )
    sales_by_product = cast_metrics(
        sales_by_product,
        float_columns=["total_revenue", "avg_price", "avg_rating", "avg_reviews"],
        long_columns=["total_quantity_sold"],
        int_columns=["product_id", "product_rank"],
    )

    sales_by_customer = (
        fact_sales.join(dim_customer, "customer_id")
        .withColumn("customer_name", F.concat_ws(" ", "first_name", "last_name"))
        .groupBy("customer_id", "customer_name", "email", "country", "age")
        .agg(
            F.count("*").alias("total_purchases"),
            F.sum("sale_total_price").alias("total_spent"),
            F.avg("sale_total_price").alias("avg_check"),
        )
        .withColumn("customer_rank", F.row_number().over(Window.orderBy(F.desc("total_spent"), F.asc("customer_id"))))
        .select(
            "customer_id",
            "customer_name",
            F.col("email").alias("customer_email"),
            F.col("country").alias("customer_country"),
            F.col("age").alias("customer_age"),
            "total_purchases",
            "total_spent",
            "avg_check",
            "customer_rank",
        )
    )
    sales_by_customer = cast_metrics(
        sales_by_customer,
        float_columns=["total_spent", "avg_check"],
        long_columns=["total_purchases"],
        int_columns=["customer_id", "customer_age", "customer_rank"],
    )

    sales_by_time = (
        fact_sales.join(dim_date, "date_id")
        .groupBy("year", "month", "month_name", "quarter")
        .agg(
            F.count("*").alias("total_sales"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_quantity").alias("avg_order_size"),
            F.avg("sale_total_price").alias("avg_order_value"),
        )
        .orderBy("year", "month")
    )
    sales_by_time = cast_metrics(
        sales_by_time,
        float_columns=["total_revenue", "avg_order_size", "avg_order_value"],
        long_columns=["total_sales"],
        int_columns=["year", "month", "quarter"],
    )

    sales_by_store = (
        fact_sales.join(dim_store, "store_id")
        .groupBy("store_id", "name", "city", "country")
        .agg(
            F.count("*").alias("total_sales"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_total_price").alias("avg_check"),
        )
        .withColumn("store_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"), F.asc("store_id"))))
        .withColumnRenamed("name", "store_name")
        .withColumnRenamed("city", "store_city")
        .withColumnRenamed("country", "store_country")
    )
    sales_by_store = cast_metrics(
        sales_by_store,
        float_columns=["total_revenue", "avg_check"],
        long_columns=["total_sales"],
        int_columns=["store_id", "store_rank"],
    )

    sales_by_supplier = (
        fact_sales.join(dim_supplier, "supplier_id")
        .join(dim_product.select("product_id", "price"), "product_id")
        .groupBy("supplier_id", "name", "country")
        .agg(
            F.sum("sale_quantity").alias("total_products_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("price").alias("avg_product_price"),
        )
        .withColumn("supplier_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"), F.asc("supplier_id"))))
        .withColumnRenamed("name", "supplier_name")
        .withColumnRenamed("country", "supplier_country")
    )
    sales_by_supplier = cast_metrics(
        sales_by_supplier,
        float_columns=["total_revenue", "avg_product_price"],
        long_columns=["total_products_sold"],
        int_columns=["supplier_id", "supplier_rank"],
    )

    product_quality_source = fact_sales.join(dim_product, "product_id")
    correlation_value = product_quality_source.select(F.corr("rating", "sale_quantity").alias("corr")).collect()[0]["corr"]
    correlation_value = float(correlation_value) if correlation_value is not None else 0.0

    product_quality = (
        product_quality_source.groupBy("product_id", "name", "category", "brand")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.sum("reviews").alias("total_reviews"),
            F.sum("sale_quantity").alias("total_sales_qty"),
            F.sum("sale_total_price").alias("total_revenue"),
        )
        .withColumn("rating_rank_best", F.row_number().over(Window.orderBy(F.desc("avg_rating"), F.asc("product_id"))))
        .withColumn("rating_rank_worst", F.row_number().over(Window.orderBy(F.asc("avg_rating"), F.asc("product_id"))))
        .withColumn("rating_sales_correlation", F.lit(correlation_value))
        .withColumnRenamed("name", "product_name")
        .withColumnRenamed("category", "product_category")
        .withColumnRenamed("brand", "product_brand")
    )
    product_quality = cast_metrics(
        product_quality,
        float_columns=["avg_rating", "total_revenue", "rating_sales_correlation"],
        long_columns=["total_reviews", "total_sales_qty"],
        int_columns=["product_id", "rating_rank_best", "rating_rank_worst"],
    )

    log("Writing marts to ClickHouse")
    sales_by_product.write.jdbc(clickhouse_url, "mart_sales_by_product", mode="append", properties=clickhouse_props)
    sales_by_customer.write.jdbc(clickhouse_url, "mart_sales_by_customer", mode="append", properties=clickhouse_props)
    sales_by_time.write.jdbc(clickhouse_url, "mart_sales_by_time", mode="append", properties=clickhouse_props)
    sales_by_store.write.jdbc(clickhouse_url, "mart_sales_by_store", mode="append", properties=clickhouse_props)
    sales_by_supplier.write.jdbc(clickhouse_url, "mart_sales_by_supplier", mode="append", properties=clickhouse_props)
    product_quality.write.jdbc(clickhouse_url, "mart_product_quality", mode="append", properties=clickhouse_props)

    log("ClickHouse marts load completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()
