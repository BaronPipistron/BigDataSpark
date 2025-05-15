
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, countDistinct


def transform_store_sales(fact: DataFrame, dim_store: DataFrame, dim_countries: DataFrame) -> DataFrame:
    """
    Считает выручку и средний чек по магазинам и городам.
    """
    store_ref = dim_countries.select("country_id", "country_name")
    df = fact.join(dim_store, "store_id").join(store_ref, "country_id")
    return df.groupBy("store_id", "city", "country_name") \
        .agg(
            _sum("total_price").alias("revenue"),
            round(_sum("total_price") / countDistinct("sale_id"), 2).alias("avg_order_value")
        )
