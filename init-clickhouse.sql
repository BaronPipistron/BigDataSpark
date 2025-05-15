CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS product_sales (
    product_id String,
    name String,
    units_sold UInt64,
    revenue Float64,
    avg_rating Float32,
    reviews_count UInt64
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS customer_sales (
    customer_id String,
    country_name String,
    total_spent Float64,
    avg_order_value Float32
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS time_sales (
    year UInt32,
    month UInt32,
    revenue Float64,
    avg_order_value Float32,
    time_id String
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS store_sales (
    store_id String,
    city String,
    country_name String,
    revenue Float64,
    avg_order_value Float32
) ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS supplier_sales (
    supplier_id String,
    country_name String,
    revenue Float64,
    avg_price Float32
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS product_quality (
    product_id String,
    name String,
    avg_rating Float32,
    reviews_count UInt64,
    units_sold UInt64
) ENGINE = MergeTree()
ORDER BY product_id;
