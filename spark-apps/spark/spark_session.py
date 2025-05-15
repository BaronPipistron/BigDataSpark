from pyspark.sql import SparkSession

def create_spark_session():
    """
    Создает и настраивает SparkSession для выполнения ETL-пайплайна.
    """
    return SparkSession.builder \
        .appName("GenerateReports") \
        .getOrCreate()
