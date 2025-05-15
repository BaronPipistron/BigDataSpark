from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, avg, count, round


def transform_product_sales(fact: DataFrame, dim_prod: DataFrame) -> DataFrame:
    """
    Считает продажи по продуктам: количество, выручка, средний рейтинг и число отзывов.
    """
    prod_supplier = dim_prod.select("product_id", "supplier_id", "name", "rating", "reviews")
    df = fact.join(prod_supplier, "product_id")
    return df.groupBy("product_id", "name").agg(
        _sum("quantity").alias("units_sold"),
        _sum("total_price").alias("revenue"),
        round(avg("rating"), 2).alias("avg_rating"),
        count("rating").alias("reviews_count")
    )
