""" Shared Spark session helper for extract/transform/load """
import os


def get_spark_session() -> 'SparkSession':
    """
    Get (or create) the Spark session used to distribute ETL work across a cluster.

    Passes HOME to YARN executor containers via spark.executorEnv.HOME so that
    Gen3Auth (which calls os.makedirs(os.path.expanduser('~') + '/.cache/...'))
    resolves to the writable hadoop user home (/home/hadoop) rather than the
    unwritable parent directory (/home) that YARN sometimes injects as HOME.
    """
    from pyspark.sql import SparkSession
    executor_home = os.environ.get('HOME', '/home/hadoop')
    return SparkSession.builder \
        .appName('gen3_etl') \
        .master(os.environ.get('SPARK_MASTER', 'local[*]')) \
        .config('spark.executorEnv.HOME', executor_home) \
        .config('spark.yarn.appMasterEnv.HOME', executor_home) \
        .getOrCreate()
