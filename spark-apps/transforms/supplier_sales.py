
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, avg


def transform_supplier_sales(fact: DataFrame, dim_prod: DataFrame, dim_supp: DataFrame, dim_countries: DataFrame) -> DataFrame:
    """
    Считает выручку и среднюю цену заказа по поставщикам.
    """
    prod_supplier = dim_prod.select("product_id", "supplier_id")
    country_ref = dim_countries.select("country_id", "country_name")
    df = fact.join(prod_supplier, "product_id") \
             .join(dim_supp, "supplier_id") \
             .join(country_ref, "country_id")
    return df.groupBy("supplier_id", "country_name") \
        .agg(
            _sum("total_price").alias("revenue"),
            round(avg("total_price"), 2).alias("avg_price")
        )
