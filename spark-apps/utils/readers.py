from pyspark.sql import DataFrame
from config import POSTGRES_URL, POSTGRES_PROPS

def read_fact_sales(spark) -> DataFrame:
    """
    Читает фактовую таблицу продаж из PostgreSQL.
    """
    return spark.read.jdbc(
        url=POSTGRES_URL,
        table="public.fact_sales",
        properties=POSTGRES_PROPS
    )


def read_dimension(spark, table_name: str) -> DataFrame:
    """
    Универсальная функция для чтения таблиц измерений по имени.
    """
    return spark.read.jdbc(
        url=POSTGRES_URL,
        table=f"public.{table_name}",
        properties=POSTGRES_PROPS
    )
