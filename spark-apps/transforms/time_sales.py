from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, countDistinct, col


def transform_time_sales(fact: DataFrame, dim_date: DataFrame) -> DataFrame:
    """
    Считает выручку и средний чек по годам и месяцам.
    """
    df = fact.join(dim_date, fact.date_id == dim_date.date_id)
    ts = df.groupBy("year", "month") \
        .agg(
        _sum("total_price").alias("revenue"),
        round(_sum("total_price") / countDistinct("sale_id"), 2).alias("avg_order_value")
    )
    return ts.filter(col("year").isNotNull() & col("month").isNotNull())
