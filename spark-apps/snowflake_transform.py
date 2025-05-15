import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_DB = "salesdb"

DB_URL = f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}"

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/"
CLICKHOUSE_PROPERTIES = {
    "user": "default",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


def load_table(session, table_name: str) -> DataFrame:
    return session.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()




def write_to_postgres(df: DataFrame, table_name: str) -> None:
    df.write.jdbc(
        url=DB_URL,
        table=table_name,
        mode="append",
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )



def snowflake_transform(dataframe: DataFrame) -> None:
    ################## dim_colors ###################

    dim_colors = dataframe.select("product_color") \
        .filter(col("product_color").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("product_color")
    dim_colors = dim_colors.withColumn("color_id", row_number().over(window=window_spec))
    dim_colors = dim_colors.withColumnRenamed("product_color", "color_name")

    write_to_postgres(dim_colors, "dim_colors")

    ################ dim_materials ##################

    dim_materials = dataframe.select("product_material") \
        .filter(col("product_material").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("product_material")
    dim_materials = dim_materials.withColumn("material_id", row_number().over(window=window_spec))
    dim_materials = dim_materials.withColumnRenamed("product_material", "material_name")

    write_to_postgres(dim_materials, "dim_materials")

    ################## dim_brands ###################

    dim_brands = dataframe.select("product_brand") \
        .filter(col("product_brand").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("product_brand")
    dim_brands = dim_brands.withColumn("brand_id", row_number().over(window=window_spec))
    dim_brands = dim_brands.withColumnRenamed("product_brand", "brand_name")

    write_to_postgres(dim_brands, "dim_brands")

    ############ dim_product_categories #############

    dim_product_categories = dataframe.select("product_category") \
        .filter(col("product_category").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("product_category")
    dim_product_categories = dim_product_categories.withColumn("category_id", row_number().over(window=window_spec))
    dim_product_categories = dim_product_categories.withColumnRenamed("product_category", "category_name")

    write_to_postgres(dim_product_categories, "dim_product_categories")

    ################ dim_countries ##################

    customer_countries = dataframe.select(col("customer_country").alias("country_name"))
    seller_countries = dataframe.select(col("seller_country").alias("country_name"))
    store_countries = dataframe.select(col("store_country").alias("country_name"))
    supplier_countries = dataframe.select(col("supplier_country").alias("country_name"))

    all_countries = customer_countries.union(seller_countries) \
        .union(store_countries) \
        .union(supplier_countries) \
        .filter(col("country_name").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("country_name")
    dim_countries = all_countries.withColumn("country_id", row_number().over(window_spec))
    dim_countries = dim_countries.select("country_id", "country_name")

    write_to_postgres(dim_countries, "dim_countries")

    ############## dim_pet_categories ###############

    dim_pet_categories = dataframe.select("pet_category") \
        .filter(col("pet_category").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("pet_category")
    dim_pet_categories = dim_pet_categories.withColumn("pet_category_id", row_number().over(window=window_spec))
    dim_pet_categories = dim_pet_categories.withColumnRenamed("pet_category", "category_name")

    write_to_postgres(dim_pet_categories, "dim_pet_categories")

    ################ dim_pet_types ##################

    dim_pet_types = dataframe.select("customer_pet_type") \
        .filter(col("customer_pet_type").isNotNull()) \
        .distinct()

    window_spec = Window.orderBy("customer_pet_type")
    dim_pet_types = dim_pet_types.withColumn("pet_type_id", row_number().over(window=window_spec))
    dim_pet_types = dim_pet_types.withColumnRenamed("customer_pet_type", "type_name")

    write_to_postgres(dim_pet_types, "dim_pet_types")

    ################## dim_date #####################

    # dim_date
    dim_date = dataframe.select("sale_date") \
    .filter(col("sale_date").isNotNull()) \
    .distinct()

    dim_date = dim_date.withColumn("day", dayofmonth("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("year", year("sale_date")) \
    .withColumn("quarter", quarter("sale_date")) \
    .withColumn("day_of_week", dayofweek("sale_date")) \
    .withColumn("is_weekend", when(col("day_of_week") >= 6, True).otherwise(False))

    window_spec = Window.orderBy("sale_date")
    dim_date = dim_date.withColumn("date_id", row_number().over(window_spec))

    # Переименовываем столбец для совпадения с DDL
    dim_date = dim_date.withColumnRenamed("sale_date", "date")

    # Записываем в PostgreSQL
    write_to_postgres(dim_date, "dim_date")


    ################ dim_suppliers ##################

    dim_suppliers = dataframe.select(
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    ).filter(col("supplier_country").isNotNull()) \

    dim_suppliers = dim_suppliers.join(
        dim_countries,
        dim_suppliers.supplier_country == dim_countries.country_name,
        "inner"
    )

    window_spec = Window.orderBy("supplier_name")
    dim_suppliers = dim_suppliers.withColumn("supplier_id", row_number().over(window_spec))

    dim_suppliers = dim_suppliers.select(
        col("supplier_id"),
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("country_id")
    )

    write_to_postgres(dim_suppliers, "dim_suppliers")

    ################# dim_stores ####################

    dim_stores = dataframe.select(
        col("store_name"),
        col("store_location"),
        col("store_city"),
        col("store_state"),
        col("store_country"),
        col("store_phone"),
        col("store_email")
    ).filter(col("store_country").isNotNull())

    dim_stores = dim_stores.join(
        dim_countries,
        dim_stores.store_country == dim_countries.country_name,
        "inner"
    )

    window_spec = Window.orderBy("store_name")
    dim_stores = dim_stores.withColumn("store_id", row_number().over(window_spec))

    dim_stores = dim_stores.select(
        col("store_id"),
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email"),
        col("country_id")
    )

    write_to_postgres(dim_stores, "dim_stores")

    ################# dim_sellers ###################

    dim_sellers = dataframe.select(
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code"
    ).filter(col("seller_country").isNotNull())

    dim_sellers = dim_sellers.join(
        dim_countries,
        dim_sellers.seller_country == dim_countries.country_name,
        "inner"
    )

    window_spec = Window.orderBy("seller_first_name", "seller_last_name")
    dim_sellers = dim_sellers.withColumn("seller_id", row_number().over(window_spec))

    dim_sellers = dim_sellers.select(
        col("seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_postal_code").alias("postal_code"),
        col("country_id")
    )

    write_to_postgres(dim_sellers, "dim_sellers")

    ################## dim_pets #####################

    dim_pets = dataframe.select(
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "pet_category"
    ).filter(
        col("customer_pet_type").isNotNull() &
        col("pet_category").isNotNull()
    )

    dim_pets = dim_pets.join(
        dim_pet_types,
        dim_pets.customer_pet_type == dim_pet_types.type_name,
        "inner"
    ).join(
        dim_pet_categories,
        dim_pets.pet_category == dim_pet_categories.category_name,
        "inner"
    )

    window_spec = Window.orderBy("customer_pet_name")
    dim_pets = dim_pets.withColumn("pet_id", row_number().over(window_spec))

    dim_pets = dim_pets.select(
        col("pet_id"),
        col("pet_type_id"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed"),
        col("pet_category_id").alias("pet_category")
    )

    write_to_postgres(dim_pets, "dim_pets")

    ################ dim_customers ##################

    dim_customers = dataframe.select(
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_name"
    ).filter(
        col("customer_country").isNotNull() &
        col("customer_pet_name").isNotNull()
    )

    dim_customers = dim_customers.join(
        dim_countries,
        dim_customers.customer_country == dim_countries.country_name,
        "inner"
    ).join(
        dim_pets,
        dim_customers.customer_pet_name == dim_pets.pet_name,
        "inner"
    )

    window_spec = Window.orderBy("customer_first_name", "customer_last_name")
    dim_customers = dim_customers.withColumn("customer_id", row_number().over(window_spec))

    dim_customers = dim_customers.select(
        col("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_postal_code").alias("postal_code"),
        col("country_id"),
        col("pet_id")
    )

    write_to_postgres(dim_customers, "dim_customers")

    ################ dim_products ###################

    dim_products = dataframe.select(
        "product_name",
        "product_category",
        "product_price",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
        "supplier_name"
    )

    dim_products = dim_products.join(
        dim_product_categories,
        dim_products.product_category == dim_product_categories.category_name,
        "inner"
    ).join(
        dim_colors,
        dim_products.product_color == dim_colors.color_name,
        "inner"
    ).join(
        dim_brands,
        dim_products.product_brand == dim_brands.brand_name,
        "inner"
    ).join(
        dim_materials,
        dim_products.product_material == dim_materials.material_name,
        "inner"
    ).join(
        dim_suppliers,
        dim_products.supplier_name == dim_suppliers.name,
        "inner"
    )

    window_spec = Window.orderBy("product_name")
    dim_products = dim_products.withColumn("product_id", row_number().over(window_spec))

    dim_products = dim_products.select(
        col("product_id"),
        col("product_name").alias("name"),
        col("category_id"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("color_id"),
        col("product_size").alias("size"),
        col("brand_id"),
        col("material_id"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date"),
        col("supplier_id")
    )

    write_to_postgres(dim_products, "dim_products")

    ################# fact_sales ####################

    fact_sales = dataframe.select(
        "sale_customer_id",
        "sale_seller_id",
        "sale_product_id",
        "store_name",
        "sale_date",
        "sale_quantity",
        "sale_total_price"
    )

    fact_sales = fact_sales.join(
        dim_customers,
        fact_sales.sale_customer_id == dim_customers.customer_id,
        "inner"
    ).join(
        dim_sellers,
        fact_sales.sale_seller_id == dim_sellers.seller_id,
        "inner"
    ).join(
        dim_products,
        fact_sales.sale_product_id == dim_products.product_id,
        "inner"
    ).join(
        dim_stores,
        fact_sales.store_name == dim_stores.name,
        "inner"
    ).join(
        dim_date,
        fact_sales.sale_date == dim_date.date,
        "inner"
    )

    window_spec = Window.orderBy("customer_id", "product_id", "sale_quantity")
    fact_sales = fact_sales.withColumn("sale_id", row_number().over(window_spec))

    fact_sales = fact_sales.select(
        col("sale_id"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("date_id"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    )

    write_to_postgres(fact_sales, "fact_sales")

spark = SparkSession.builder \
    .appName("Spark ETL") \
    .getOrCreate()

raw_df = load_table(spark, "mock_data")

snowflake_transform(raw_df)
