CREATE KEYSPACE IF NOT EXISTS sales
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sales;

CREATE TABLE IF NOT EXISTS product_sales (
    product_id text PRIMARY KEY,
    name text,
    units_sold bigint,
    revenue double,
    avg_rating float,
    reviews_count int
);

CREATE TABLE IF NOT EXISTS customer_sales (
    customer_id text PRIMARY KEY,
    country_name text,
    total_spent double,
    avg_order_value float
);

CREATE TABLE IF NOT EXISTS time_sales (
    time_id text PRIMARY KEY,
    year int,
    month int,
    revenue double,
    avg_order_value float
);

CREATE TABLE IF NOT EXISTS store_sales (
    store_id text PRIMARY KEY,
    city text,
    country_name text,
    revenue double,
    avg_order_value float
);

CREATE TABLE IF NOT EXISTS supplier_sales (
    supplier_id text PRIMARY KEY,
    country_name text,
    revenue double,
    avg_price float
);

CREATE TABLE IF NOT EXISTS product_quality (
    product_id text PRIMARY KEY,
    name text,
    avg_rating float,
    reviews_count int,
    units_sold bigint
);
