
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, round, countDistinct


def transform_customer_sales(fact: DataFrame, dim_cust: DataFrame, dim_countries: DataFrame) -> DataFrame:
    """
    Считает продажи по клиентам: общая сумма и средний чек.
    """
    df = fact.join(dim_cust, "customer_id").join(dim_countries, "country_id")
    return df.groupBy("customer_id", "country_name") \
        .agg(
            _sum("total_price").alias("total_spent"),
            round(_sum("total_price") / countDistinct("sale_id"), 2).alias("avg_order_value")
        )