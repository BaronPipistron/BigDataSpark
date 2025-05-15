
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, avg, count


def transform_product_quality(fact: DataFrame, dim_prod: DataFrame) -> DataFrame:
    """
    Считает качество продуктов: средний рейтинг, число отзывов, количество проданных единиц.
    """
    prod = dim_prod.select("product_id", "name", "rating", "reviews")
    df = fact.join(prod, "product_id")
    return df.groupBy("product_id", "name") \
        .agg(
            round(avg("rating"), 2).alias("avg_rating"),
            count("reviews").alias("reviews_count"),
            _sum("quantity").alias("units_sold")
        )