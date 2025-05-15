from pyspark.sql.functions import concat_ws, col
from spark.spark_session import create_spark_session
from utils.readers import read_fact_sales, read_dimension
from transforms.product_sales import transform_product_sales
from transforms.customer_sales import transform_customer_sales
from transforms.time_sales import transform_time_sales
from transforms.store_sales import transform_store_sales
from transforms.supplier_sales import transform_supplier_sales
from transforms.product_quality import transform_product_quality
from utils.writer import (
    write_to_clickhouse,
    write_to_cassandra,
    write_to_mongodb,
    write_to_neo4j,
    write_to_valkey
)


spark = create_spark_session()

fact = read_fact_sales(spark)
dim_prod = read_dimension(spark, "dim_products")
dim_cust = read_dimension(spark, "dim_customers")
dim_store = read_dimension(spark, "dim_stores")
dim_supp = read_dimension(spark, "dim_suppliers")
dim_date = read_dimension(spark, "dim_date")
dim_countries = read_dimension(spark, "dim_countries")

product_sales = transform_product_sales(fact, dim_prod)
customer_sales = transform_customer_sales(fact, dim_cust, dim_countries)
time_sales = transform_time_sales(fact, dim_date)
store_sales = transform_store_sales(fact, dim_store, dim_countries)
supplier_sales = transform_supplier_sales(fact, dim_prod, dim_supp, dim_countries)
product_quality = transform_product_quality(fact, dim_prod)

# ClickHouse
write_to_clickhouse(product_sales, "product_sales")
write_to_clickhouse(customer_sales, "customer_sales")
write_to_clickhouse(time_sales, "time_sales")
write_to_clickhouse(store_sales, "store_sales")
write_to_clickhouse(supplier_sales, "supplier_sales")
write_to_clickhouse(product_quality, "product_quality")

# Cassandra
write_to_cassandra(product_sales, "product_sales")
write_to_cassandra(customer_sales, "customer_sales")
write_to_cassandra(time_sales, "time_sales")
write_to_cassandra(store_sales, "store_sales")
write_to_cassandra(supplier_sales, "supplier_sales")
write_to_cassandra(product_quality, "product_quality")

# MongoDB
write_to_mongodb(product_sales, collection="product_sales")
write_to_mongodb(customer_sales, collection="customer_sales")
write_to_mongodb(time_sales, collection="time_sales")
write_to_mongodb(store_sales, collection="store_sales")
write_to_mongodb(supplier_sales, collection="supplier_sales")
write_to_mongodb(product_quality, collection="product_quality")

# Neo4j
write_to_neo4j(product_sales, node_label="product_sales", key_field="product_id")
write_to_neo4j(customer_sales, node_label="customer_sales", key_field="customer_id")

time_sales = time_sales.withColumn("time_id", concat_ws("-", col("year"), col("month")))
write_to_neo4j(time_sales, node_label="time_sales", key_field="time_id")
time_sales = transform_time_sales(fact, dim_date)

write_to_neo4j(store_sales, node_label="store_sales", key_field="store_id")
write_to_neo4j(supplier_sales, node_label="supplier_sales", key_field="supplier_id")
write_to_neo4j(product_quality, node_label="product_quality", key_field="product_id")

# Valkey
write_to_valkey(product_sales, key_prefix="product_sales")
write_to_valkey(customer_sales, key_prefix="customer_sales")
write_to_valkey(time_sales, key_prefix="time_sales")
write_to_valkey(store_sales, key_prefix="store_sales")
write_to_valkey(supplier_sales, key_prefix="supplier_sales")
write_to_valkey(product_quality, key_prefix="product_quality")

spark.stop()
