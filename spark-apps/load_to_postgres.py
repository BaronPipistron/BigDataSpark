from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType
)
from pyspark.sql.functions import (
    col, to_date, regexp_replace
)
import os

spark = SparkSession.builder \
    .appName('LoadToPostgres') \
    .getOrCreate()

data_dir = 'prepared'
csv_path = os.path.join(data_dir, 'mock*.csv')

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('customer_first_name', StringType(), True),
    StructField('customer_last_name', StringType(), True),
    StructField('customer_age', IntegerType(), True),
    StructField('customer_email', StringType(), True),
    StructField('customer_country', StringType(), True),
    StructField('customer_postal_code', StringType(), True),
    StructField('customer_pet_type', StringType(), True),
    StructField('customer_pet_name', StringType(), True),
    StructField('customer_pet_breed', StringType(), True),
    StructField('seller_first_name', StringType(), True),
    StructField('seller_last_name', StringType(), True),
    StructField('seller_email', StringType(), True),
    StructField('seller_country', StringType(), True),
    StructField('seller_postal_code', StringType(), True),
    StructField('product_name', StringType(), True),
    StructField('product_category', StringType(), True),
    StructField('product_price', DoubleType(), True),
    StructField('product_quantity', IntegerType(), True),
    StructField('sale_date', StringType(), True),
    StructField('sale_customer_id', IntegerType(), True),
    StructField('sale_seller_id', IntegerType(), True),
    StructField('sale_product_id', IntegerType(), True),
    StructField('sale_quantity', IntegerType(), True),
    StructField('sale_total_price', DoubleType(), True),
    StructField('store_name', StringType(), True),
    StructField('store_location', StringType(), True),
    StructField('store_city', StringType(), True),
    StructField('store_state', StringType(), True),
    StructField('store_country', StringType(), True),
    StructField('store_phone', StringType(), True),
    StructField('store_email', StringType(), True),
    StructField('pet_category', StringType(), True),
    StructField('product_weight', DoubleType(), True),
    StructField('product_color', StringType(), True),
    StructField('product_size', StringType(), True),
    StructField('product_brand', StringType(), True),
    StructField('product_material', StringType(), True),
    StructField('product_description', StringType(), True),
    StructField('product_rating', DoubleType(), True),
    StructField('product_reviews', IntegerType(), True),
    StructField('product_release_date', StringType(), True),
    StructField('product_expiry_date', StringType(), True),
    StructField('supplier_name', StringType(), True),
    StructField('supplier_contact', StringType(), True),
    StructField('supplier_email', StringType(), True),
    StructField('supplier_phone', StringType(), True),
    StructField('supplier_address', StringType(), True),
    StructField('supplier_city', StringType(), True),
    StructField('supplier_country', StringType(), True),
])

df_raw = spark.read \
    .option('header', 'true') \
    .schema(schema) \
    .csv(csv_path)

df_clean = df_raw.withColumn(
    'product_description',
    regexp_replace(col('product_description'), r'[\r\n]+', ' ')
)

df = (
    df_clean
    .withColumn('sale_date', to_date(col('sale_date'), 'M/d/yyyy'))
    .withColumn('product_release_date', to_date(col('product_release_date'), 'M/d/yyyy'))
    .withColumn('product_expiry_date', to_date(col('product_expiry_date'), 'M/d/yyyy'))
)

jdbc_url = 'jdbc:postgresql://postgres:5432/salesdb'
pg_props = {
    'user': 'user',
    'password': 'password',
    'driver': 'org.postgresql.Driver'
}

df.write \
    .jdbc(
        url=jdbc_url,
        table='public.mock_data',
        mode='overwrite',
        properties=pg_props
    )

spark.stop()
