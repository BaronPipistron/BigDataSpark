import os

from dotenv import load_dotenv
from py4j.java_gateway import java_import

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

DOTENV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

if os.path.exists(DOTENV_PATH):
    load_dotenv(DOTENV_PATH)
else:
    print("ERROR: Could not find the .env file.")
    exit(1)

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')

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
        .load()


def write_to_postgres(dataframe: DataFrame, table_name: str) -> None:
    dataframe.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()


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
    dim_date = dim_date.withColumnRenamed("sale_date", "date")

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


from pyspark.sql import SparkSession
from py4j.java_gateway import java_import


def create_clickhouse_connection(spark):
    """Создает подключение к ClickHouse через явное создание драйвера"""
    try:
        # Инициализация Java классов
        java_import(spark.sparkContext._jvm, "java.sql.*")
        java_import(spark.sparkContext._jvm, "java.util.Properties")

        # Явная загрузка и создание драйвера
        driver_class = spark.sparkContext._jvm.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
        driver = driver_class.newInstance()

        # Создание URL и свойств подключения
        url = "jdbc:clickhouse://clickhouse:8123/"
        props = spark.sparkContext._jvm.java.util.Properties()
        props.setProperty("user", "default")

        # Создание подключения напрямую через драйвер
        conn = driver.connect(url, props)
        return conn

    except Exception as e:
        print(f"Ошибка при создании подключения: {str(e)}")
        raise


def create_clickhouse_base(spark, db_name: str) -> None:
    """Создает базу данных в ClickHouse"""
    conn = None
    try:
        conn = create_clickhouse_connection(spark)
        stmt = conn.createStatement()
        stmt.executeUpdate(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"База данных {db_name} успешно создана")
    except Exception as e:
        print(f"Ошибка при создании базы: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def write_to_clickhouse(spark, df, table_name, db_name="product_analysis"):
    """Записывает DataFrame в таблицу ClickHouse"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://clickhouse:8123/{db_name}") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("dbtable", table_name) \
            .option("user", "default") \
            .mode("append") \
            .save()
        print(f"Данные записаны в {db_name}.{table_name}")
    except Exception as e:
        print(f"Ошибка при записи данных: {str(e)}")
        raise


def generate_showcase(session) -> None:
    ################# showcase 1 ####################

    fact_sales = load_table(session, "fact_sales")
    dim_products = load_table(session, "dim_products")
    dim_categories = load_table(session, "dim_product_categories")

    top_products = fact_sales.join(dim_products, "product_id") \
        .groupBy("product_id", "name") \
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue"),
            count("*").alias("sales_count")
        ).orderBy(desc("total_quantity")).limit(10)

    revenue_by_category = fact_sales.join(dim_products, "product_id") \
        .join(dim_categories, "category_id") \
        .groupBy("category_id", "category_name") \
        .agg(
            sum("total_price").alias("category_revenue"),
            round(avg(dim_products["rating"]), 2).alias("avg_rating")
        ).orderBy(desc("category_revenue"))

    products_with_reviews = dim_products.join(
        fact_sales.groupBy("product_id").agg(
            sum("quantity").alias("total_quantity"),
            sum("total_price").alias("total_revenue")
        ),
        "product_id", "left"
    ).select(
        "product_id",
        "name",
        "rating",
        "reviews",
        "total_quantity",
        "total_revenue"
    )

    create_clickhouse_base(session, "product_analysis")

    ddl_queries = [
        """
        CREATE TABLE IF NOT EXISTS product_analysis.top_products (
            product_id Int32,
            name String,
            total_quantity Int32,
            total_revenue Decimal(18,2),
            sales_count Int32,
            load_date Date DEFAULT today()
        ) ENGINE = MergeTree()
        ORDER BY (total_quantity, product_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS product_analysis.revenue_by_category (
            category_id Int32,
            category_name String,
            category_revenue Decimal(18,2),
            avg_rating Float32,
            load_date Date DEFAULT today()
        ) ENGINE = MergeTree()
        ORDER BY (category_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS product_analysis.products_with_reviews (
            product_id Int32,
            name String,
            rating Float32,
            reviews Int32,
            total_quantity Int32,
            total_revenue Decimal(18,2),
            load_date Date DEFAULT today()
        ) ENGINE = MergeTree()
        ORDER BY (product_id)
        """
    ]

    conn = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_PROPERTIES)

    for query in ddl_queries:
        conn.createStatement().executeUpdate(query)

    conn.close()

    write_to_clickhouse(top_products, "product_analysis", "top_products")
    write_to_clickhouse(revenue_by_category, "product_analysis", "revenue_by_category")
    write_to_clickhouse(products_with_reviews, "product_analysis", "products_with_reviews")

    ################# showcase 2 ####################

    ################# showcase 3 ####################

    ################# showcase 4 ####################

    ################# showcase 5 ####################

    ################# showcase 6 ####################


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark ETL") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.4.jar") \
        .config("spark.jars", "/opt/spark/jars/spark-cassandra-connector_2.12-3.4.0.jar") \
        .config("spark.jars", "/opt/spark/jars/neo4j-connector-apache-spark_2.12-5.0.0.jar") \
        .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar") \
        .config("spark.jars", "/opt/spark/jars/spark-redis_2.12-3.1.0.jar") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar") \
        .config("spark.hadoop.security.authentication", "simple") \
        .getOrCreate()

    raw_df = load_table(spark, "raw_data")

    #snowflake_transform(raw_df)

    generate_showcase(spark)