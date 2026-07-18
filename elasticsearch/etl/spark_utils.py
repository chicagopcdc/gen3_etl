""" Shared Spark session helper for extract/transform/load """
import os


def get_spark_session() -> any:
    """ Get (or create) the Spark session used to distribute ETL work across a cluster """
    from pyspark.sql import SparkSession 
    return SparkSession.builder \
        .appName('gen3_etl') \
        .master(os.environ.get('SPARK_MASTER', 'local[*]')) \
        .getOrCreate()
